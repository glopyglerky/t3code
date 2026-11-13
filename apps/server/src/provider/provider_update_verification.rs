//! Provider refresh decisions used before and after an update command.
//!
//! The application observes providers and owns updates, locks and state publication.
//! These decisions preserve `providerMaintenanceRunner.ts`'s refreshed-version rules.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSnapshot {
    pub driver: Option<String>,
    pub checked_at: String,
    pub installed: bool,
    pub status: String,
    pub auth_status: String,
    pub version: Option<String>,
    /// None means omitted; Some(None) means explicitly unknown, without legacy fallback.
    pub installed_version: Option<Option<String>>,
    pub version_advisory_status: Option<String>,
}

pub fn already_current(
    prior_checked_at: Option<&str>,
    advisory_enriched: bool,
    providers: &[ProviderSnapshot],
) -> bool {
    let Some(prior_checked_at) = prior_checked_at else {
        return false;
    };
    advisory_enriched
        && !providers.is_empty()
        && providers.iter().all(|provider| {
            provider.checked_at != prior_checked_at
                && provider.installed
                && provider.status != "error"
                && provider.version_advisory_status.as_deref() == Some("current")
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateVerification {
    CouldNotVerify,
    StillOutdated,
    Succeeded,
    RestartOpenCodeServer,
}

impl UpdateVerification {
    pub fn status(self) -> &'static str {
        match self {
            Self::CouldNotVerify | Self::StillOutdated => "unchanged",
            Self::Succeeded | Self::RestartOpenCodeServer => "succeeded",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::CouldNotVerify => {
                "Update command completed, but T3 Code could not verify the provider version."
            }
            Self::StillOutdated => {
                "Update command completed, but T3 Code still detects an outdated provider version."
            }
            Self::Succeeded => "Provider updated.",
            Self::RestartOpenCodeServer => {
                "OpenCode CLI updated. Restart the configured local OpenCode server to use the new version."
            }
        }
    }
}

pub fn verify_update(
    target_version: Option<&str>,
    providers: &[ProviderSnapshot],
) -> UpdateVerification {
    let could_not_verify = providers.is_empty()
        || providers.iter().any(|provider| {
            let installed_version = match &provider.installed_version {
                Some(version) => version.as_deref(),
                None => provider.version.as_deref(),
            };
            !provider.installed
                || (provider.status == "error"
                    && (provider.auth_status != "unauthenticated"
                        || provider.version.as_ref().is_none_or(String::is_empty)))
                || target_version.is_some_and(|target| {
                    installed_version.map(|version| version.strip_prefix('v').unwrap_or(version))
                        != Some(target)
                })
        });
    if could_not_verify {
        return UpdateVerification::CouldNotVerify;
    }
    let still_outdated = target_version.is_none()
        && providers
            .iter()
            .any(|provider| provider.version_advisory_status.as_deref() == Some("behind_latest"));
    if still_outdated {
        UpdateVerification::StillOutdated
    } else if providers.iter().any(|provider| {
        provider.driver.as_deref() == Some("opencode")
            && provider.version.is_some()
            && provider
                .installed_version
                .as_ref()
                .is_some_and(|installed| installed.is_some() && installed != &provider.version)
    }) {
        UpdateVerification::RestartOpenCodeServer
    } else {
        UpdateVerification::Succeeded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> ProviderSnapshot {
        ProviderSnapshot {
            driver: None,
            checked_at: "2026-11-10T00:00:01Z".to_string(),
            installed: true,
            status: "ready".to_string(),
            auth_status: "authenticated".to_string(),
            version: Some("1.2.3".to_string()),
            installed_version: None,
            version_advisory_status: Some("current".to_string()),
        }
    }

    #[test]
    fn precheck_requires_a_fresh_successful_refresh_of_every_provider() {
        let provider = snapshot();
        let prior = Some("2026-11-10T00:00:00Z");
        assert!(already_current(prior, true, &[provider.clone()]));
        assert!(!already_current(None, true, &[provider.clone()]));
        assert!(!already_current(prior, false, &[provider.clone()]));
        assert!(!already_current(prior, true, &[]));
        for other in [
            ProviderSnapshot {
                checked_at: prior.unwrap().to_string(),
                ..provider.clone()
            },
            ProviderSnapshot {
                installed: false,
                ..provider.clone()
            },
            ProviderSnapshot {
                status: "error".to_string(),
                ..provider.clone()
            },
            ProviderSnapshot {
                version_advisory_status: None,
                ..provider.clone()
            },
            ProviderSnapshot {
                version_advisory_status: Some("behind_latest".to_string()),
                ..provider.clone()
            },
        ] {
            assert!(!already_current(prior, true, &[provider.clone(), other]));
        }
        // Presence matters; an empty prior timestamp is still a prior record.
        assert!(already_current(Some(""), true, &[provider]));
    }

    #[test]
    fn installed_version_distinguishes_omitted_null_and_empty() {
        let provider = snapshot();
        assert_eq!(
            verify_update(Some("1.2.3"), &[provider.clone()]),
            UpdateVerification::Succeeded
        );
        let unknown = ProviderSnapshot {
            installed_version: Some(None),
            ..provider.clone()
        };
        assert_eq!(
            verify_update(Some("1.2.3"), &[unknown.clone()]),
            UpdateVerification::CouldNotVerify
        );
        assert_eq!(
            verify_update(None, &[unknown]),
            UpdateVerification::Succeeded
        );
        let empty = ProviderSnapshot {
            installed_version: Some(Some(String::new())),
            ..provider
        };
        assert_eq!(
            verify_update(Some("1.2.3"), &[empty.clone()]),
            UpdateVerification::CouldNotVerify
        );
        assert_eq!(
            verify_update(Some(""), &[empty]),
            UpdateVerification::Succeeded
        );
    }

    #[test]
    fn targeted_updates_remove_exactly_one_lowercase_prefix_and_ignore_latest_status() {
        for (version, expected) in [
            ("v1.2.3", UpdateVerification::Succeeded),
            ("1.2.3", UpdateVerification::Succeeded),
            ("vv1.2.3", UpdateVerification::CouldNotVerify),
            ("V1.2.3", UpdateVerification::CouldNotVerify),
            ("1.2.3\n", UpdateVerification::CouldNotVerify),
        ] {
            let provider = ProviderSnapshot {
                installed_version: Some(Some(version.to_string())),
                version_advisory_status: Some("behind_latest".to_string()),
                ..snapshot()
            };
            assert_eq!(verify_update(Some("1.2.3"), &[provider]), expected);
        }
    }

    #[test]
    fn unauthenticated_errors_require_a_nonempty_reported_version() {
        let mut provider = ProviderSnapshot {
            status: "error".to_string(),
            auth_status: "unauthenticated".to_string(),
            installed_version: Some(Some("1.2.3".to_string())),
            ..snapshot()
        };
        assert_eq!(
            verify_update(Some("1.2.3"), &[provider.clone()]),
            UpdateVerification::Succeeded
        );
        for version in [None, Some(String::new())] {
            provider.version = version;
            assert_eq!(
                verify_update(None, &[provider.clone()]),
                UpdateVerification::CouldNotVerify
            );
        }
        provider.version = Some(" ".to_string());
        assert_eq!(
            verify_update(None, &[provider.clone()]),
            UpdateVerification::Succeeded
        );
        provider.auth_status = "authenticated".to_string();
        assert_eq!(
            verify_update(None, &[provider]),
            UpdateVerification::CouldNotVerify
        );
    }

    #[test]
    fn missing_installation_takes_precedence_over_outdated_advisory() {
        let outdated = ProviderSnapshot {
            version_advisory_status: Some("behind_latest".to_string()),
            ..snapshot()
        };
        assert_eq!(verify_update(None, &[]), UpdateVerification::CouldNotVerify);
        assert_eq!(
            verify_update(None, &[snapshot(), outdated.clone()]),
            UpdateVerification::StillOutdated
        );
        let missing = ProviderSnapshot {
            installed: false,
            ..snapshot()
        };
        for providers in [[outdated.clone(), missing.clone()], [missing, outdated]] {
            assert_eq!(
                verify_update(None, &providers),
                UpdateVerification::CouldNotVerify
            );
        }
        assert_eq!(UpdateVerification::CouldNotVerify.status(), "unchanged");
        assert_eq!(UpdateVerification::StillOutdated.status(), "unchanged");
        assert_eq!(UpdateVerification::Succeeded.status(), "succeeded");
    }
}
