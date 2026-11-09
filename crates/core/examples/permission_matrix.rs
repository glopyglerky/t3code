use t3_code_ls_core::permissions::{
    ALL_PERMISSIONS, ClientSession, Permission, legacy_scope_response, required_legacy_scope,
};

fn selection(index: usize) -> Vec<Permission> {
    if index == 0 {
        Vec::new()
    } else if index <= ALL_PERMISSIONS.len() {
        vec![ALL_PERMISSIONS[index - 1]]
    } else if index == ALL_PERMISSIONS.len() + 1 {
        ALL_PERMISSIONS.to_vec()
    } else {
        vec![
            Permission::OrchestrationRead,
            Permission::OrchestrationOperate,
            Permission::TerminalOperate,
            Permission::OrchestrationOperate,
        ]
    }
}

fn main() {
    for permission in ALL_PERMISSIONS {
        println!(
            "scope\t{}\t{}",
            permission.as_str(),
            required_legacy_scope(permission).as_str()
        );
    }
    for permission in legacy_scope_response(&ALL_PERMISSIONS) {
        println!("legacy\t{}", permission.as_str());
    }
    for authenticated in [false, true] {
        for has_server_update_scope in [false, true] {
            for scopes in 0..ALL_PERMISSIONS.len() + 3 {
                for permissions in 0..ALL_PERMISSIONS.len() + 4 {
                    let session = ClientSession {
                        authenticated,
                        scopes: selection(scopes),
                        permissions: if permissions == 0 {
                            None
                        } else {
                            Some(selection(permissions - 1))
                        },
                        has_server_update_scope,
                    };
                    let mut grants = String::new();
                    for permission in ALL_PERMISSIONS {
                        grants.push(if session.grants(permission) { '1' } else { '0' });
                    }
                    println!("{}\t{}", grants, session.has_legacy_permissions());
                }
            }
        }
    }
}
