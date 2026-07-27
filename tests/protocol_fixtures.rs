use polymarket_rs::market_ws::{market_subscription_payload, MarketSubscription};
use polymarket_rs::types::{AssetId, ConditionId};
use polymarket_rs::user_ws::{
    user_subscription_payload_live, user_subscription_payload_redacted, UserSubscription,
};

#[test]
fn market_payload_matches_expected_shape() {
    let payload =
        market_subscription_payload(&MarketSubscription::new(vec![AssetId::from("asset-a")]));
    assert_eq!(
        payload,
        "{\"type\":\"market\",\"assets_ids\":[\"asset-a\"],\"custom_feature_enabled\":true}"
    );
}

#[test]
fn user_payload_is_condition_id_based() {
    let sub = UserSubscription::new(
        vec![ConditionId::from("condition-1")],
        "key",
        "secret",
        "passphrase",
    );
    let payload = user_subscription_payload_redacted(&sub);
    assert!(payload.contains("\"type\":\"user\""));
    assert!(payload.contains("\"markets\":[\"condition-1\"]"));
    assert_eq!(
        payload,
        "{\"type\":\"user\",\"markets\":[\"condition-1\"],\"auth\":{\"apiKey\":\"<redacted>\",\"secret\":\"<redacted>\",\"passphrase\":\"<redacted>\"}}"
    );
    assert!(user_subscription_payload_live(&sub).is_err());
}

#[test]
fn fixture_documents_price_changes_array_shape() {
    let fixture = include_str!("../fixtures/ws/price_changes_multi_asset.json");
    assert!(fixture.contains("\"price_changes\""));
    assert!(fixture.contains("\"hash\": \"hash-a\""));
}
