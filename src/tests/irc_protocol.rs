//! End-to-end IRC protocol tests using deterministic server transcripts.

use crate::agent::AgentId;
use crate::irc::{
    capabilities::{
        CapabilityAction, CapabilityNegotiator, CapabilityStatus, CompatibilityCatalog,
        NegotiationPhase, SaslMechanism, SaslPolicy,
    },
    codec::{InboundFrame, IrcCodec, MalformedReason, OutboundFrame},
    commands::{ResponseStrategy, spec_for, strategy_for},
    correlation::{CommandId, CommandOutcome, Correlator, CorrelatorLimits, PendingCommand},
    isupport::{CaseMapping, IsupportRegistry},
    registration::{NickConflictPolicy, Nickname, NicknamePlan, NicknameRejection},
    semantic::{MembershipChange, SemanticClass, SemanticEvent, project},
    wire::{LineBudget, OutboundMessage, ParseStatus, WireMessage},
};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

fn decode_all(codec: &mut IrcCodec, transcript: &[u8]) -> Vec<WireMessage> {
    let mut source = BytesMut::from(transcript);
    let mut messages = Vec::new();
    while let Some(frame) = codec.decode(&mut source).expect("decode") {
        match frame {
            InboundFrame::Message(message) => messages.push(*message),
            InboundFrame::Malformed(line) => panic!("unexpected malformed line: {line:?}"),
        }
    }
    messages
}

const REGISTRATION: &[u8] = b"\
:server CAP * LS * :server-time message-tags batch labeled-response\r\n\
:server CAP * LS :echo-message account-tag multi-prefix draft/chathistory sasl=PLAIN\r\n\
:server CAP * ACK :server-time message-tags batch labeled-response echo-message account-tag multi-prefix draft/chathistory\r\n\
:server 001 Kuebiko :Welcome to the network\r\n\
:server 005 Kuebiko NICKLEN=32 CHANNELLEN=64 CASEMAPPING=ascii PREFIX=(qaohv)~&@%+ CHANTYPES=#& LINELEN=1024 TARGMAX=PRIVMSG:4 :are supported\r\n\
:server 375 Kuebiko :- server Message of the Day -\r\n\
:server 372 Kuebiko :- be excellent to each other\r\n\
:server 376 Kuebiko :End of /MOTD command.\r\n";

#[test]
fn a_registration_transcript_negotiates_and_configures_the_connection() {
    let mut codec = IrcCodec::new(LineBudget::TRADITIONAL, 8 * 1024);
    let mut negotiator = CapabilityNegotiator::new();
    let mut isupport = IsupportRegistry::new();
    let mut actions = Vec::new();

    for message in decode_all(&mut codec, REGISTRATION) {
        let action = negotiator.apply(&message);
        if action != CapabilityAction::None {
            actions.push(action);
        }
        if message.numeric() == Some(5) {
            // ISUPPORT tokens lie between the target nickname and description.
            isupport.apply_tokens(message.params.iter().skip(1).map(String::as_str));
        }
    }

    assert_eq!(actions.len(), 2);
    let CapabilityAction::Request(request) = &actions[0] else {
        panic!("expected a capability request first");
    };
    assert!(request.contains(&"labeled-response".to_owned()));
    assert!(request.contains(&"draft/chathistory".to_owned()));
    assert!(
        !request.contains(&"sasl".to_owned()),
        "guest must not request sasl"
    );
    assert_eq!(actions[1], CapabilityAction::EndNegotiation);
    assert_eq!(negotiator.phase(), NegotiationPhase::Complete);

    assert!(negotiator.is_active("echo-message"));
    assert!(negotiator.is_active("chathistory"));
    assert_eq!(
        negotiator.entries()["sasl"].status,
        CapabilityStatus::ObservedUnnegotiated
    );

    assert_eq!(isupport.nick_len(), 32);
    assert_eq!(isupport.case_mapping(), CaseMapping::Ascii);
    assert_eq!(isupport.max_targets("PRIVMSG"), Some(4));
    assert_eq!(isupport.membership_prefixes().len(), 5);

    codec.set_budget(isupport.line_budget());
    assert_eq!(codec.budget().max_body_bytes, 1_024);
}

#[test]
fn negotiated_capabilities_select_the_collector_for_each_command() {
    let mut negotiator = CapabilityNegotiator::new();
    for message in decode_all(&mut IrcCodec::traditional(), REGISTRATION) {
        negotiator.apply(&message);
    }

    assert_eq!(
        strategy_for("PRIVMSG", &negotiator),
        ResponseStrategy::Echo {
            commands: &["PRIVMSG"]
        }
    );
    assert!(!negotiator.is_active("multiline"));

    let mut guest = CapabilityNegotiator::new();
    guest.apply(
        &WireMessage::parse(bytes::Bytes::from_static(b":server CAP * LS :batch")).expect("parse"),
    );
    assert_eq!(
        strategy_for("PRIVMSG", &guest),
        ResponseStrategy::Unconfirmed
    );
    assert_eq!(
        spec_for("PRIVMSG")
            .expect("PRIVMSG")
            .missing_capabilities(&guest),
        ["echo-message"]
    );
}

#[test]
fn a_sasl_transcript_authenticates_before_ending_negotiation() {
    let transcript: &[u8] = b"\
:server CAP * LS :sasl=PLAIN,EXTERNAL batch\r\n\
:server CAP * ACK :sasl batch\r\n\
AUTHENTICATE +\r\n\
:server 900 Kuebiko Kuebiko!u@h kuebiko :You are now logged in\r\n\
:server 903 Kuebiko :SASL authentication successful\r\n";

    let mut negotiator =
        CapabilityNegotiator::with_sasl(SaslPolicy::Authenticate(SaslMechanism::Plain));
    let actions: Vec<CapabilityAction> = decode_all(&mut IrcCodec::traditional(), transcript)
        .iter()
        .map(|message| negotiator.apply(message))
        .filter(|action| *action != CapabilityAction::None)
        .collect();

    assert_eq!(
        actions,
        vec![
            CapabilityAction::Request(vec!["batch".into(), "sasl".into()]),
            CapabilityAction::Authenticate(SaslMechanism::Plain),
            CapabilityAction::SendAuthenticationPayload,
            CapabilityAction::EndNegotiation,
        ]
    );
    assert!(negotiator.is_complete());
}

#[test]
fn a_query_correlates_its_replies_and_projects_the_events() {
    let transcript: &[u8] = b"\
@label=cmd_whois :server 311 Kuebiko alice ~alice host * :Alice\r\n\
:bob!u@h PRIVMSG #room :unrelated chatter\r\n\
@label=cmd_whois :server 318 Kuebiko alice :End of /WHOIS\r\n";

    let mut isupport = IsupportRegistry::new();
    isupport.apply_tokens(["CHANTYPES=#&"]);
    let mut correlator = Correlator::new(CorrelatorLimits::default());
    correlator.set_nickname(Nickname::new("Kuebiko").expect("nickname"));

    let command_id = CommandId::new();
    correlator
        .register(PendingCommand {
            command_id: command_id.clone(),
            agent_id: AgentId::new(),
            command: "WHOIS".into(),
            label: Some("cmd_whois".into()),
            response: spec_for("WHOIS").expect("WHOIS").response,
            written: false,
            deadline_ms: 30_000,
            warnings: Vec::new(),
            replies: Vec::new(),
        })
        .expect("register");
    correlator.record_write(&command_id, true);

    let mut completions = Vec::new();
    let mut classes = Vec::new();
    for message in decode_all(&mut IrcCodec::traditional(), transcript) {
        completions.extend(correlator.ingest(&message));
        classes.push(project(&message, &isupport).class);
    }

    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].outcome, CommandOutcome::Completed);
    assert_eq!(completions[0].replies.len(), 2);
    assert_eq!(
        classes,
        [
            SemanticClass::ProtocolReply,
            SemanticClass::MessageChannel,
            SemanticClass::ProtocolReply
        ]
    );
}

#[test]
fn a_hostile_transcript_does_not_take_down_the_connection() {
    let mut codec = IrcCodec::new(LineBudget::with_body(128), 8 * 1024);
    let mut transcript = Vec::new();
    transcript.extend_from_slice(b":server 001 Kuebiko :Welcome\r\n");
    transcript.extend_from_slice(b"PRIVMSG #room :");
    transcript.extend_from_slice(&vec![b'x'; 20_000]);
    transcript.extend_from_slice(b"\r\n");
    transcript.extend_from_slice(b"PRIVMSG #room :bad\0line\r\n");
    transcript.extend_from_slice(b":nick!u@h PRIVMSG #room :caf\xe9\r\n");
    transcript.extend_from_slice(b":server PING :still-alive\r\n");

    let mut source = BytesMut::from(&transcript[..]);
    let mut messages = Vec::new();
    let mut malformed = Vec::new();
    while let Some(frame) = codec.decode(&mut source).expect("decode never errors") {
        match frame {
            InboundFrame::Message(message) => messages.push(*message),
            InboundFrame::Malformed(line) => malformed.push(line),
        }
    }

    assert_eq!(malformed.len(), 2);
    assert!(matches!(
        malformed[0].reason,
        MalformedReason::TooLong { .. }
    ));
    assert_eq!(malformed[1].reason, MalformedReason::EmbeddedControl);

    let commands: Vec<&str> = messages.iter().map(|m| m.command.as_str()).collect();
    assert_eq!(commands, ["001", "PRIVMSG", "PING"]);
    assert_eq!(messages[1].parse_status, ParseStatus::Partial);
    assert_eq!(messages[1].params, ["#room"]);
    assert!(messages[1].raw_base64.is_some());
}

#[test]
fn nickname_collisions_walk_the_plan_until_one_is_accepted() {
    let mut isupport = IsupportRegistry::new();
    isupport.apply_tokens(["NICKLEN=9"]);
    let requested = Nickname::new("Kuebiko").expect("nickname");
    let mut plan = NicknamePlan::new(
        &requested,
        &[Nickname::new("Gersemi").expect("fallback")],
        NickConflictPolicy::Suffix,
        isupport.nick_len(),
        4,
    );

    let mut sent = Vec::new();
    let transcript: &[u8] = b"\
:server 433 * Kuebiko :Nickname is already in use\r\n\
:server 433 * Gersemi :Nickname is already in use\r\n\
:server 001 Kuebiko_2 :Welcome\r\n";

    sent.push(plan.next_candidate().expect("first").to_string());
    for message in decode_all(&mut IrcCodec::traditional(), transcript) {
        let Some(numeric) = message.numeric() else {
            continue;
        };
        if let Some(rejection) = NicknameRejection::from_numeric(numeric) {
            assert!(rejection.is_retriable());
            sent.push(plan.next_candidate().expect("candidate").to_string());
        }
    }

    assert_eq!(sent, ["Kuebiko", "Gersemi", "Kuebiko_2"]);
    assert!(plan.adjusted());
    assert!(!plan.is_exhausted());
}

#[test]
fn an_outbound_command_encodes_within_the_negotiated_budget() {
    let mut isupport = IsupportRegistry::new();
    isupport.apply_tokens(["LINELEN=512"]);
    let mut codec = IrcCodec::new(isupport.line_budget(), 8 * 1024);

    let mut destination = BytesMut::new();
    codec
        .encode(
            OutboundFrame {
                message: OutboundMessage::new("PRIVMSG", vec!["#room".into()])
                    .with_trailing("hello 🦀")
                    .with_tag("+reply", Some("id 1".into())),
                label: Some("cmd_send".into()),
            },
            &mut destination,
        )
        .expect("encode");

    let line = destination.split_to(destination.len() - 2);
    let parsed = WireMessage::parse(line.freeze()).expect("parse");
    assert_eq!(parsed.command, "PRIVMSG");
    assert_eq!(parsed.tag_value("label"), Some("cmd_send"));
    assert_eq!(parsed.tag_value("+reply"), Some("id 1"));
    assert_eq!(parsed.trailing.as_deref(), Some("hello 🦀"));
}

#[test]
fn channel_traffic_projects_into_typed_events() {
    let transcript: &[u8] = b"\
@account=grant :grant!~grant@host PRIVMSG #room :status?\r\n\
:Kuebiko!u@h JOIN #room\r\n\
:op!u@h KICK #room spammer :spam\r\n\
:server TOPIC #room :Coordination\r\n";

    let mut isupport = IsupportRegistry::new();
    isupport.apply_tokens(["CHANTYPES=#&"]);
    let projections: Vec<SemanticEvent> = decode_all(&mut IrcCodec::traditional(), transcript)
        .iter()
        .map(|message| project(message, &isupport).event)
        .collect();

    let SemanticEvent::MessageChannel { source, text, .. } = &projections[0] else {
        panic!("expected a channel message");
    };
    assert_eq!(source.account.as_deref(), Some("grant"));
    assert_eq!(text, "status?");

    assert!(matches!(
        projections[1],
        SemanticEvent::Membership {
            change: MembershipChange::Joined,
            ..
        }
    ));
    assert!(matches!(
        projections[2],
        SemanticEvent::Membership {
            change: MembershipChange::Kicked,
            ..
        }
    ));
    assert!(matches!(projections[3], SemanticEvent::ChannelState { .. }));
}

#[test]
fn the_compatibility_catalog_reflects_what_was_negotiated() {
    let mut negotiator = CapabilityNegotiator::new();
    let mut isupport = IsupportRegistry::new();
    for message in decode_all(&mut IrcCodec::traditional(), REGISTRATION) {
        negotiator.apply(&message);
        if message.numeric() == Some(5) {
            isupport.apply_tokens(message.params.iter().skip(1).map(String::as_str));
        }
    }

    let mut catalog = CompatibilityCatalog::with_static_registry();
    negotiator.publish_into(&mut catalog);
    catalog.isupport = isupport.tokens().clone();
    catalog.record_discovered_command("NPCHAT");

    assert_eq!(
        catalog.capabilities["echo-message"].status,
        CapabilityStatus::Negotiated
    );
    assert_eq!(
        catalog.capabilities["draft/chathistory"].feature,
        "chathistory"
    );
    assert_eq!(catalog.isupport["NICKLEN"].value.as_deref(), Some("32"));
    assert!(catalog.commands.contains_key("NPCHAT"));
    assert!(catalog.commands.contains_key("PING"));
}
