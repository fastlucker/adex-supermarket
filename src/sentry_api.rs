use chrono::Utc;
use futures::future::{try_join_all, TryFutureExt};
use primitives::{
    sentry::{
        ChannelListResponse, LastApprovedResponse, ValidatorMessage, ValidatorMessageResponse,
    },
    Channel, ValidatorDesc,
};
use reqwest::{Client, Error, Response};
use std::time::Duration;
use url::Url;

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

    pub async fn get_validator_channels(&self, validator: &Url) -> Result<Vec<Channel>, Error> {
        let first_page = self.fetch_page(&validator, 0).await?;

        if first_page.total_pages < 2 {
            Ok(first_page.channels)
        } else {
            let all: Vec<ChannelListResponse> =
                try_join_all((1..first_page.total_pages).map(|i| self.fetch_page(&validator, i)))
                    .await?;

            let result_all: Vec<Channel> = std::iter::once(first_page)
                .chain(all.into_iter())
                .flat_map(|ch| ch.channels.into_iter())
                .collect();
            Ok(result_all)
        }
    }

    async fn fetch_page(
        &self,
        validator: &Url,
        page: u64,
    ) -> Result<ChannelListResponse, reqwest::Error> {
        let url = format!(
            "{}/channel/list?page={}&validUntil={}",
            validator,
            page,
            Utc::now()
        );

        self.client
            .get(&url)
            .send()
            .and_then(|res: Response| res.json::<ChannelListResponse>())
            .await
    }

    pub async fn get_last_approved(
        &self,
        validator: &ValidatorDesc,
    ) -> Result<LastApprovedResponse, Error> {
        let url = format!("{}/last-approved?withHeartbeat=true", validator.url);
        let response = self.client.get(&url).send().await?;

        response.json().await
    }

    pub async fn get_latest_new_state(
        &self,
        validator: &ValidatorDesc,
    ) -> Result<Option<ValidatorMessage>, Error> {
        let url = format!(
            "{}/validator-messages/{}/NewState?limit=1",
            validator.url, validator.id
        );
        let response = self.client.get(&url).send().await?;
        let response: ValidatorMessageResponse = response.json().await?;
        let message = response.validator_messages.into_iter().next();
        Ok(message)
    }
}
