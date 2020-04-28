use primitives::{sentry::LastApprovedResponse, ValidatorDesc};
use reqwest::{Client, Error};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SentryApi {
    client: Client,
}

/// SentryApi talks directly to Sentry
impl SentryApi {
    pub fn new(request_timeout: Duration) -> Result<Self, Error> {
        let client = Client::builder().timeout(request_timeout).build()?;

        Ok(Self { client })
    }

    pub async fn get_last_approved(
        &self,
        validator: &ValidatorDesc,
    ) -> Result<LastApprovedResponse, Error> {
        let url = format!("{}/last-approved?withHeartbeat=true", validator.url);
        let response = self.client.get(&url).send().await?;

        response.json().await
    }
}
