//! Port of packages/contracts/src/auth.ts at the pinned T3 nightly.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    OrchestrationRead,
    OrchestrationOperate,
    SettingsWrite,
    ProvidersManage,
    EnvironmentMaintain,
    PreviewOperate,
    DiagnosticsRead,
    TerminalRead,
    TerminalOperate,
    FilesystemRead,
    FilesystemWrite,
    ReviewWrite,
    SourceControlWrite,
    AccessRead,
    AccessWrite,
    RelayRead,
    RelayWrite,
}

pub const ALL_PERMISSIONS: [Permission; 17] = [
    Permission::OrchestrationRead,
    Permission::OrchestrationOperate,
    Permission::SettingsWrite,
    Permission::ProvidersManage,
    Permission::EnvironmentMaintain,
    Permission::PreviewOperate,
    Permission::DiagnosticsRead,
    Permission::TerminalRead,
    Permission::TerminalOperate,
    Permission::FilesystemRead,
    Permission::FilesystemWrite,
    Permission::ReviewWrite,
    Permission::SourceControlWrite,
    Permission::AccessRead,
    Permission::AccessWrite,
    Permission::RelayRead,
    Permission::RelayWrite,
];

impl Permission {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OrchestrationRead => "orchestration:read",
            Self::OrchestrationOperate => "orchestration:operate",
            Self::SettingsWrite => "settings:write",
            Self::ProvidersManage => "providers:manage",
            Self::EnvironmentMaintain => "environment:maintain",
            Self::PreviewOperate => "preview:operate",
            Self::DiagnosticsRead => "diagnostics:read",
            Self::TerminalRead => "terminal:read",
            Self::TerminalOperate => "terminal:operate",
            Self::FilesystemRead => "filesystem:read",
            Self::FilesystemWrite => "filesystem:write",
            Self::ReviewWrite => "review:write",
            Self::SourceControlWrite => "source-control:write",
            Self::AccessRead => "access:read",
            Self::AccessWrite => "access:write",
            Self::RelayRead => "relay:read",
            Self::RelayWrite => "relay:write",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        ALL_PERMISSIONS
            .into_iter()
            .find(|permission| permission.as_str() == value)
    }

    pub fn legacy_parent(self) -> Option<Self> {
        match self {
            Self::FilesystemRead | Self::DiagnosticsRead => Some(Self::OrchestrationRead),
            Self::SettingsWrite
            | Self::ProvidersManage
            | Self::EnvironmentMaintain
            | Self::PreviewOperate
            | Self::SourceControlWrite
            | Self::FilesystemWrite => Some(Self::OrchestrationOperate),
            Self::TerminalRead => Some(Self::TerminalOperate),
            _ => None,
        }
    }

    pub fn is_legacy(self) -> bool {
        matches!(
            self,
            Self::OrchestrationRead
                | Self::OrchestrationOperate
                | Self::TerminalOperate
                | Self::ReviewWrite
                | Self::AccessRead
                | Self::AccessWrite
                | Self::RelayRead
                | Self::RelayWrite
        )
    }

    pub fn can_issue(self) -> bool {
        self != Self::ReviewWrite
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClientSession {
    pub authenticated: bool,
    pub scopes: Vec<Permission>,
    // Some(empty) is authoritative denial, including decoded unknown permissions.
    pub permissions: Option<Vec<Permission>>,
    pub has_server_update_scope: bool,
}

impl ClientSession {
    /// Client display compatibility only. Server authorization must use exact grants.
    pub fn grants(&self, permission: Permission) -> bool {
        if !self.authenticated {
            return false;
        }
        if let Some(permissions) = &self.permissions {
            return permissions.contains(&permission);
        }
        if self.scopes.contains(&permission) {
            return true;
        }
        if self.has_server_update_scope {
            return false;
        }
        match permission.legacy_parent() {
            Some(parent) => self.scopes.contains(&parent),
            None => false,
        }
    }

    pub fn has_legacy_permissions(&self) -> bool {
        if !self.authenticated {
            return false;
        }
        let Some(permissions) = &self.permissions else {
            return false;
        };
        for permission in permissions {
            if !permission.is_legacy() {
                return false;
            }
        }
        permissions.contains(&Permission::OrchestrationRead)
            || permissions.contains(&Permission::OrchestrationOperate)
            || permissions.contains(&Permission::TerminalOperate)
    }
}

pub fn server_grants_permission(
    authenticated: bool,
    grants: &[Permission],
    requested: Permission,
) -> bool {
    authenticated && grants.contains(&requested)
}

pub fn decode_permissions(values: &[&str]) -> Vec<Permission> {
    let mut permissions = Vec::new();
    for value in values {
        if let Some(permission) = Permission::parse(value) {
            permissions.push(permission);
        }
    }
    permissions
}

pub fn legacy_scope_response(permissions: &[Permission]) -> Vec<Permission> {
    let mut scopes = Vec::new();
    for permission in permissions {
        if permission.is_legacy() {
            scopes.push(*permission);
        }
    }
    scopes
}

pub fn required_legacy_scope(permission: Permission) -> Permission {
    permission.legacy_parent().unwrap_or(permission)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_empty_and_unknown_permissions_never_expand_legacy_authority() {
        let mut session = ClientSession {
            authenticated: true,
            scopes: vec![Permission::OrchestrationOperate],
            permissions: Some(decode_permissions(&["future:permission"])),
            has_server_update_scope: false,
        };
        for permission in ALL_PERMISSIONS {
            assert!(!session.grants(permission));
        }
        session.permissions = None;
        assert!(session.grants(Permission::SettingsWrite));
        session.has_server_update_scope = true;
        assert!(!session.grants(Permission::SettingsWrite));
        assert!(session.grants(Permission::OrchestrationOperate));
        session.authenticated = false;
        assert!(!session.grants(Permission::OrchestrationOperate));
    }

    #[test]
    fn legacy_compatibility_cannot_be_used_as_a_server_grant() {
        let grants = vec![Permission::OrchestrationOperate];
        for requested in ALL_PERMISSIONS {
            assert_eq!(
                server_grants_permission(true, &grants, requested),
                requested == Permission::OrchestrationOperate
            );
            assert!(!server_grants_permission(
                false,
                &ALL_PERMISSIONS,
                requested
            ));
            assert!(required_legacy_scope(requested).is_legacy());
        }
        assert_eq!(
            Permission::parse("review:write"),
            Some(Permission::ReviewWrite)
        );
        assert!(!Permission::ReviewWrite.can_issue());
        assert!(Permission::SourceControlWrite.can_issue());
    }

    #[test]
    fn explicit_grants_override_legacy_scopes_and_preserve_response_order() {
        let session = ClientSession {
            authenticated: true,
            scopes: vec![Permission::OrchestrationOperate],
            permissions: Some(vec![Permission::FilesystemRead]),
            has_server_update_scope: false,
        };
        assert!(session.grants(Permission::FilesystemRead));
        assert!(!session.grants(Permission::SettingsWrite));
        assert!(!session.has_legacy_permissions());
        assert_eq!(
            legacy_scope_response(&[
                Permission::RelayWrite,
                Permission::FilesystemRead,
                Permission::RelayRead,
                Permission::RelayWrite,
            ]),
            vec![
                Permission::RelayWrite,
                Permission::RelayRead,
                Permission::RelayWrite
            ]
        );
    }
}
