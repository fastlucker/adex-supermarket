use primitives::{sentry::LastApprovedResponse, ValidatorDesc};
use reqwest::{Client, Error};

#[derive(Debug, Clone)]
pub struct SentryApi {
    client: Client,
}

/// SentryApi talks directly to Sentry
impl SentryApi {
    pub fn new() -> Result<Self, Error> {
        let client = Client::builder().build()?;

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
