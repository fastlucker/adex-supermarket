use chrono::Utc;
use futures::future::{try_join_all, TryFutureExt};
use primitives::{
    sentry::{
        channel_list::ChannelListQuery, ChannelListResponse, LastApprovedResponse,
        ValidatorMessage, ValidatorMessageResponse,
    },
    Channel, ValidatorDesc,
};
use reqwest::{Client, Response};
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Debug, Clone)]
pub struct SentryApi {
    client: Client,
}
#[derive(Debug, Error)]
pub enum Error {
    #[error("Parsing Validator Url: {0}")]
    ParsingUrl(#[from] url::ParseError),
    #[error("Request to Sentry: {0}")]
    Reqwest(#[from] reqwest::Error),
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

    async fn fetch_page(&self, validator: &Url, page: u64) -> Result<ChannelListResponse, Error> {
        let query = ChannelListQuery {
            page,
            valid_until_ge: Utc::now(),
            creator: None,
            validator: None,
        };

        let url = format!(
            "{}/channel/list?{}",
            validator.to_string().trim_end_matches('/'),
            serde_urlencoded::to_string(&query).expect("Should serialize")
        );

        Ok(self
            .client
            .get(&url)
            .send()
            .and_then(|res: Response| res.json::<ChannelListResponse>())
            .await?)
    }

    pub async fn get_last_approved(
        &self,
        validator: &ValidatorDesc,
    ) -> Result<LastApprovedResponse, Error> {
        let url = format!(
            "{}/last-approved?withHeartbeat=true",
            validator.url.trim_end_matches('/')
        );

        Ok(self.client.get(&url).send().await?.json().await?)
    }

    pub async fn get_latest_new_state(
        &self,
        validator: &ValidatorDesc,
    ) -> Result<Option<ValidatorMessage>, Error> {
        let url = &format!(
            "{}/validator-messages/{}/NewState?limit=1",
            validator.url.trim_end_matches('/'),
            validator.id
        );

        let response: ValidatorMessageResponse = self.client.get(url).send().await?.json().await?;
        let message = response.validator_messages.into_iter().next();

        Ok(message)
    }
}
