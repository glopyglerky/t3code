use std::io::{self, BufRead};
use t3_code_ls_core::json::{self, Value};
use t3_code_ls_core::provider_recovery::{self, NativeSubagentEnd, RootTurnsEnd};
use t3_code_ls_core::provider_recovery_wire::{self as wire, Error, WireResult};

fn field<'a>(value: &'a Value, key: &str) -> WireResult<&'a Value> {
    value.get(key).ok_or(Error::InvalidRequest)
}

fn text(value: &Value) -> WireResult<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or(Error::InvalidRequest)
}

fn timestamp(value: &Value) -> WireResult<i64> {
    value.as_i64().ok_or(Error::InvalidRequest)
}

fn nullable<T>(value: &Value, read: impl FnOnce(&Value) -> WireResult<T>) -> WireResult<Option<T>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        read(value).map(Some)
    }
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}
fn time_value(value: &i64) -> Value {
    Value::Number(value.to_string())
}

fn evaluate(input: &Value) -> WireResult<Value> {
    let source = field(input, "projection")?;
    let projection = wire::decode_projection(source, timestamp)?;
    let before = projection.clone();
    let emitted = wire::decode_emitted(field(input, "emitted")?)?;
    let completion = wire::decode_completion(input, timestamp)?;
    let failure_at = input
        .get("failureAt")
        .map(|value| value.as_u64().ok_or(Error::InvalidRequest))
        .transpose()?;
    let allocation_start = input
        .get("allocationStart")
        .map(|value| value.as_u64().ok_or(Error::InvalidRequest))
        .transpose()?
        .unwrap_or(0);
    let mut allocations = 0u64;
    let mut allocate = || {
        let attempt = allocations;
        allocations += 1;
        if failure_at == Some(attempt) {
            Err(attempt)
        } else {
            Ok(format!("event:recovery:{}", allocation_start + attempt))
        }
    };
    let result = match field(input, "operation")?.as_str() {
        Some("orphan") => provider_recovery::end_orphaned_native_subagent(
            &projection,
            NativeSubagentEnd {
                subagent_id: field(input, "subagentId")?
                    .as_str()
                    .ok_or(Error::InvalidRequest)?,
                completion,
                result: input
                    .get("result")
                    .map(|value| nullable(value, text))
                    .transpose()?,
            },
            &emitted,
            &mut allocate,
        ),
        Some("roots") => provider_recovery::end_runless_root_turns(
            &projection,
            RootTurnsEnd {
                thread_id: text(field(input, "threadId")?)?,
                provider_instance_id: text(field(input, "providerInstanceId")?)?,
                completion,
            },
            &emitted,
            &mut allocate,
        ),
        _ => return Err(Error::InvalidRequest),
    };
    assert_eq!(projection, before, "planner mutated its projection");
    let outcome = match result {
        Ok(events) => (
            "events",
            Value::Array(
                events
                    .iter()
                    .map(|event| wire::encode_event(event, time_value))
                    .collect(),
            ),
        ),
        Err(attempt) => (
            "error",
            object(vec![
                ("tag", Value::String("AllocationFailure".to_string())),
                ("attempt", Value::Number(attempt.to_string())),
            ]),
        ),
    };
    Ok(object(vec![
        outcome,
        ("allocations", Value::Number(allocations.to_string())),
    ]))
}

fn main() {
    for input in io::stdin().lock().lines() {
        let input = input.expect("read recovery fixture");
        let value = json::parse(&input).expect("parse recovery fixture");
        println!(
            "{}",
            json::stringify(&evaluate(&value).expect("evaluate recovery fixture"))
        );
    }
}
