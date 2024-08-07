// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use chrono::format::parse_and_remainder;
use chrono::format::Parsed;
use chrono::format::StrftimeItems;
use chrono::prelude::*;
use chrono::Datelike;
use chrono::Days;
use chrono::Duration;
use chrono::MappedLocalTime;
use chrono::Utc;
use chrono_tz::Tz;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use databend_common_ast::ParseError;
use databend_common_exception::ErrorCode;
use databend_common_expression::error_to_null;
use databend_common_expression::types::date::check_date;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt16Type;
use databend_common_expression::types::number::UInt32Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::timestamp::MICROS_IN_A_MILLI;
use databend_common_expression::types::timestamp::MICROS_IN_A_SEC;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::F64;
use databend_common_expression::utils::date_helper::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use databend_common_io::cursor_ext::unwrap_local_time;
use dtparse::parse;
use num_traits::AsPrimitive;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS timestamp)
    // to_timestamp(xx)
    register_string_to_timestamp(registry);
    register_date_to_timestamp(registry);
    register_number_to_timestamp(registry);

    // cast(xx AS date)
    // to_date(xx)
    register_string_to_date(registry);
    register_timestamp_to_date(registry);
    register_number_to_date(registry);

    // cast([date | timestamp] AS string)
    // to_string([date | timestamp])
    register_to_string(registry);

    // cast([date | timestamp] AS [uint8 | int8 | ...])
    // to_[uint8 | int8 | ...]([date | timestamp])
    register_to_number(registry);

    // [add | subtract]_[years | months | days | hours | minutes | seconds]([date | timestamp], number)
    // date_[add | sub]([year | quarter | month | week | day | hour | minute | second], [date | timestamp], number)
    // [date | timestamp] [+ | -] interval number [year | quarter | month | week | day | hour | minute | second]
    register_add_functions(registry);
    register_sub_functions(registry);

    // now, today, yesterday, tomorrow
    register_real_time_functions(registry);

    // to_*([date | timestamp]) -> number
    register_to_number_functions(registry);

    // to_*([date | timestamp]) -> [date | timestamp]
    register_rounder_functions(registry);

    // [date | timestamp] +/- number
    register_timestamp_add_sub(registry);

    // convert_timezone( target_timezone, date, src_timezone)
    register_convert_timezone(registry);
}

/// Check if timestamp is within range, and return the timestamp in micros.
#[inline]
fn int64_to_timestamp(n: i64) -> Result<i64, String> {
    if -31536000000 < n && n < 31536000000 {
        Ok(n * MICROS_IN_A_SEC)
    } else if -31536000000000 < n && n < 31536000000000 {
        Ok(n * MICROS_IN_A_MILLI)
    } else {
        check_timestamp(n)
    }
}

fn int64_domain_to_timestamp_domain<T: AsPrimitive<i64>>(
    domain: &SimpleDomain<T>,
) -> Option<SimpleDomain<i64>> {
    Some(SimpleDomain {
        min: int64_to_timestamp(domain.min.as_()).ok()?,
        max: int64_to_timestamp(domain.max.as_()).ok()?,
    })
}

fn register_convert_timezone(registry: &mut FunctionRegistry) {
    // 2 arguments function [target_timezone, src_timestamp]
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, TimestampType, _, _>(
        "convert_timezone",
        |_, _, _| FunctionDomain::MayThrow,
        eval_convert_timezone,
    );

    // 3 arguments function [target_timezone, src_timestamp, src_timezone]
    registry.register_passthrough_nullable_3_arg::<StringType, StringType, StringType, TimestampType, _, _>(
        "convert_timezone",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<StringType, StringType, StringType, TimestampType>(
            |target_tz, src_timestamp, src_tz, output, ctx| {
                // Parsing parameters
                let t_tz: Tz = match target_tz.parse() {
                    Ok(tz) => tz,
                    Err(e) => {
                        return ctx.set_error(
                            output.len(),
                            format!("cannot parse target `timezone`. {}", e),
                        );
                    }
                };

                let s_tz: Tz = match src_tz.parse() {
                    Ok(tz) => tz,
                    Err(e) => {
                        return ctx.set_error(
                            output.len(),
                            format!("cannot parse src `timezone`. {}", e),
                        );
                    }
                };
                // Parsing src_timestamp
                let result_timestamp: Result<DateTime<Tz>, ParseError> = match src_timestamp.parse::<DateTime<Utc>>() {
                    Ok(utc_dt) => Ok(utc_dt.with_timezone(&s_tz)),
                    Err(_) => Ok(src_timestamp.parse::<NaiveDateTime>().unwrap().and_local_timezone(s_tz).unwrap()),
                };

                output.push(result_timestamp.unwrap().with_timezone(&t_tz).timestamp_micros())
            },
        ),
    );

    fn eval_convert_timezone(
        target_tz: ValueRef<StringType>,
        src_timestamp: ValueRef<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_2_arg::<StringType, StringType, TimestampType>(
            |target_tz, src_timestamp, output, ctx| {
                // Parsing parameters
                let t_tz: Tz = match target_tz.parse() {
                    Ok(tz) => tz,
                    Err(e) => {
                        return ctx.set_error(
                            output.len(),
                            format!("cannot parse target `timezone`. {}", e),
                        );
                    }
                };

                // Split the src_timestamp, isolate this timezone if given
                let str_datetime_part: Vec<&str> = src_timestamp.split(' ').collect();
                if str_datetime_part.len() != 3 {
                    return ctx.set_error(
                        output.len(),
                        format!("Invalid src_timestamp format : {}", src_timestamp),
                    );
                }

                let (datetime_part, offset_part) =
                    src_timestamp.rsplit_once(' ').expect("Invalid format");

                let str_naive_datetime = datetime_part;
                let naive_datetime =
                    match NaiveDateTime::parse_from_str(str_naive_datetime, "%Y-%m-%d %H:%M:%S") {
                        Ok(parsed) => parsed,
                        Err(e) => {
                            return ctx.set_error(
                                output.len(),
                                format!("Unable to parse src_timestamp : {}", e),
                            );
                        }
                    };

                let str_offset = offset_part;
                // Process the offset
                let offset_sign = &str_offset[0..1]; // Get the sign of the offset (+ or -)
                let offset_time = &str_offset[1..]; // Get the time part of the offset
                let offset_parts: Vec<&str> = offset_time.split(':').collect();
                let hours: i64 = offset_parts[0].parse().expect("Invalid number for hours");
                let minutes: i64 = offset_parts[1].parse().expect("Invalid number for minutes");

                // Convert offset to seconds
                let total_seconds = hours * 3600 + minutes * 60;
                let sec_offset = match offset_sign {
                    "+" => Duration::seconds(-total_seconds),
                    "-" => Duration::seconds(total_seconds),
                    _ => {
                        return ctx.set_error(output.len(), format!("Invalid offset sign"));
                    }
                };

                // Add the offset to the naive datetime
                let adjusted_datetime = naive_datetime
                    .checked_add_signed(sec_offset)
                    .expect("Overflow when adding offset");
                let utc_adjusted_datetime = Utc.from_utc_datetime(&adjusted_datetime);
                output.push(
                    utc_adjusted_datetime
                        .with_timezone(&t_tz)
                        .timestamp_micros(),
                )
            },
        )(target_tz, src_timestamp, ctx)
    }
}
fn register_string_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_date", &["str_to_date", "date"]);
    registry.register_aliases("to_year", &["str_to_year", "year"]);
    registry.register_aliases("to_day_of_month", &["day", "dayofmonth"]);
    registry.register_aliases("to_day_of_year", &["dayofyear"]);
    registry.register_aliases("to_month", &["month"]);
    registry.register_aliases("to_quarter", &["quarter"]);
    registry.register_aliases("to_week_of_year", &["week", "weekofyear"]);

    registry.register_aliases("to_timestamp", &["to_datetime", "str_to_timestamp"]);
    registry.register_aliases("try_to_timestamp", &["try_to_datetime"]);

    registry.register_passthrough_nullable_1_arg::<StringType, TimestampType, _, _>(
        "to_timestamp",
        |_, _| FunctionDomain::MayThrow,
        eval_string_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<StringType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_timestamp),
    );

    fn eval_string_to_timestamp(
        val: ValueRef<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<StringType, TimestampType>(|val, output, ctx| {
            let tz = ctx.func_ctx.tz.tz;
            let enable_dst_hour_fix = ctx.func_ctx.enable_dst_hour_fix;
            if ctx.func_ctx.enable_strict_datetime_parser {
                match string_to_timestamp(val, tz, enable_dst_hour_fix) {
                    Ok(ts) => output.push(ts.timestamp_micros()),
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("cannot parse to type `TIMESTAMP`. {}", e),
                        );
                        output.push(0);
                    }
                }
            } else {
                match parse(val) {
                    Ok((naive_dt, parse_tz)) => {
                        if let Some(parse_tz) = parse_tz {
                            match naive_dt.and_local_timezone(parse_tz) {
                                MappedLocalTime::Single(res) => {
                                    output.push(res.with_timezone(&tz).timestamp_micros())
                                }
                                MappedLocalTime::None => {
                                    if enable_dst_hour_fix {
                                        if let Some(res2) =
                                            naive_dt.checked_add_signed(Duration::seconds(3600))
                                        {
                                            match tz.from_local_datetime(&res2) {
                                                MappedLocalTime::Single(t) => {
                                                    output.push(t.timestamp_micros())
                                                }
                                                MappedLocalTime::Ambiguous(t1, _) => {
                                                    output.push(t1.timestamp_micros())
                                                }
                                                MappedLocalTime::None => {
                                                    let err = format!(
                                                        "Local Time Error: The local time {:?}, {} can not map to a single unique result with timezone {}",
                                                        naive_dt, res2, tz
                                                    );
                                                    ctx.set_error(
                                                        output.len(),
                                                        format!(
                                                            "cannot parse to type `TIMESTAMP`. {}",
                                                            err
                                                        ),
                                                    );
                                                    output.push(0);
                                                }
                                            }
                                        }
                                    } else {
                                        let err = format!(
                                            "The time {:?} can not map to a single unique result with timezone {}",
                                            naive_dt, tz
                                        );
                                        ctx.set_error(
                                            output.len(),
                                            format!("cannot parse to type `TIMESTAMP`. {}", err),
                                        );
                                        output.push(0);
                                    }
                                }
                                MappedLocalTime::Ambiguous(t1, t2) => {
                                    if enable_dst_hour_fix {
                                        output.push(t1.with_timezone(&tz).timestamp_micros());
                                    } else {
                                        output.push(t2.with_timezone(&tz).timestamp_micros());
                                    }
                                }
                            }
                        } else {
                            match unwrap_local_time(&tz, enable_dst_hour_fix, &naive_dt) {
                                Ok(res) => output.push(res.timestamp_micros()),
                                Err(e) => {
                                    ctx.set_error(
                                        output.len(),
                                        format!("cannot parse to type `TIMESTAMP`. {}", e),
                                    );
                                    output.push(0);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        ctx.set_error(
                            output.len(),
                            format!("cannot parse to type `TIMESTAMP`. {}", err),
                        );
                        output.push(0);
                    }
                }
            }
        })(val, ctx)
    }

    registry.register_combine_nullable_2_arg::<StringType, StringType, TimestampType, _, _>(
        "to_timestamp",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<TimestampType>>(
            |timestamp, format, output, ctx| match string_to_format_timestamp(
                timestamp, format, ctx,
            ) {
                Ok((ts, need_null)) => {
                    if need_null {
                        output.push_null();
                    } else {
                        output.push(ts);
                    }
                }
                Err(e) => {
                    ctx.set_error(output.len(), e.to_string());
                    output.push_null();
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<TimestampType>>(
            |timestamp, format, output, ctx| match string_to_format_timestamp(
                timestamp, format, ctx,
            ) {
                Ok((ts, need_null)) => {
                    if need_null {
                        output.push_null();
                    } else {
                        output.push(ts);
                    }
                }
                Err(_) => {
                    output.push_null();
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, DateType, _, _>(
        "to_date",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<DateType>>(
            |date, format, output, ctx| {
                if format.is_empty() {
                    output.push_null();
                } else {
                    match NaiveDate::parse_from_str(date, format) {
                        Ok(res) => {
                            output.push(res.num_days_from_ce() - EPOCH_DAYS_FROM_CE);
                        }
                        Err(e) => {
                            ctx.set_error(output.len(), e.to_string());
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, DateType, _, _>(
        "try_to_date",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<DateType>>(
            |date, format, output, _| {
                if format.is_empty() {
                    output.push_null();
                } else {
                    match NaiveDate::parse_from_str(date, format) {
                        Ok(res) => {
                            output.push(res.num_days_from_ce() - EPOCH_DAYS_FROM_CE);
                        }
                        Err(_) => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );
}

fn string_to_format_timestamp(
    timestamp: &str,
    format: &str,
    ctx: &mut EvalContext,
) -> Result<(i64, bool), ErrorCode> {
    if format.is_empty() {
        return Ok((0, true));
    }
    // Parse with extra checks for timezone
    // %Z	ACST	Local time zone name. Skips all non-whitespace characters during parsing. Identical to %:z when formatting. 6
    // %z	+0930	Offset from the local time to UTC (with UTC being +0000).
    // %:z	+09:30	Same as %z but with a colon.
    // %::z	+09:30:00	Offset from the local time to UTC with seconds.
    // %:::z	+09	Offset from the local time to UTC without minutes.
    // %#z	+09	Parsing only: Same as %z but allows minutes to be missing or present.
    let timezone_strftime = ["%Z", "%z", "%:z", "%::z", "%:::z", "%#z"];
    let parse_tz = timezone_strftime
        .iter()
        .any(|&pattern| format.contains(pattern));
    let enable_dst_hour_fix = ctx.func_ctx.enable_dst_hour_fix;
    let tz = ctx.func_ctx.tz.tz;
    if ctx.func_ctx.parse_datetime_ignore_remainder {
        let mut parsed = Parsed::new();
        if let Err(e) = parse_and_remainder(&mut parsed, timestamp, StrftimeItems::new(format)) {
            return Err(ErrorCode::BadArguments(format!("{}", e)));
        }
        // Additional checks and adjustments for parsed timestamp
        // If parsed.timestamp is Some no need to pad default year.
        if parsed.timestamp.is_none() {
            if parsed.year.is_none() {
                parsed.year = Some(1970);
                parsed.year_div_100 = Some(19);
                parsed.year_mod_100 = Some(70);
            }
            if parsed.month.is_none() {
                parsed.month = Some(1);
            }
            if parsed.day.is_none() {
                parsed.day = Some(1);
            }
            if parsed.hour_div_12.is_none() && parsed.hour_mod_12.is_none() {
                parsed.hour_div_12 = Some(0);
                parsed.hour_mod_12 = Some(0);
            }
            if parsed.minute.is_none() {
                parsed.minute = Some(0);
            }
            if parsed.second.is_none() {
                parsed.second = Some(0);
            }
        }

        if parse_tz {
            parsed.offset.get_or_insert(0);
            parsed
                .to_datetime()
                .map(|res| (res.timestamp_micros(), false))
                .map_err(|err| ErrorCode::BadArguments(format!("{err}")))
        } else {
            parsed
                .to_naive_datetime_with_offset(0)
                .map_err(|err| ErrorCode::BadArguments(format!("{err}")))
                .and_then(
                    |res| match unwrap_local_time(&tz, enable_dst_hour_fix, &res) {
                        Ok(res) => Ok((res.timestamp_micros(), false)),
                        Err(e) => Err(e),
                    },
                )
        }
    } else if parse_tz {
        DateTime::parse_from_str(timestamp, format)
            .map(|res| (res.timestamp_micros(), false))
            .map_err(|err| ErrorCode::BadArguments(format!("{}", err)))
    } else {
        NaiveDateTime::parse_from_str(timestamp, format)
            .map_err(|err| ErrorCode::BadArguments(format!("{}", err)))
            .and_then(
                |res| match unwrap_local_time(&tz, enable_dst_hour_fix, &res) {
                    Ok(res) => Ok((res.timestamp_micros(), false)),
                    Err(e) => Err(e),
                },
            )
    }
}

fn register_date_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, TimestampType, _, _>(
        "to_timestamp",
        |ctx, domain| {
            let tz = ctx.tz.tz;
            FunctionDomain::Domain(SimpleDomain {
                min: calc_date_to_timestamp(domain.min, tz),
                max: calc_date_to_timestamp(domain.max, tz),
            })
        },
        eval_date_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<DateType, TimestampType, _, _>(
        "try_to_timestamp",
        |ctx, domain| {
            let tz = ctx.tz.tz;
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain {
                    min: calc_date_to_timestamp(domain.min, tz),
                    max: calc_date_to_timestamp(domain.max, tz),
                })),
            })
        },
        error_to_null(eval_date_to_timestamp),
    );

    fn eval_date_to_timestamp(
        val: ValueRef<DateType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<DateType, TimestampType>(|val, output, _| {
            let tz = ctx.func_ctx.tz.tz;
            output.push(calc_date_to_timestamp(val, tz));
        })(val, ctx)
    }

    fn calc_date_to_timestamp(val: i32, tz: Tz) -> i64 {
        let ts = (val as i64) * 24 * 3600 * MICROS_IN_A_SEC;
        let epoch_time_with_ltz = tz
            .from_utc_datetime(
                &NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_micro_opt(0, 0, 0, 0)
                    .unwrap(),
            )
            .naive_local()
            .and_utc()
            .timestamp_micros();

        ts - epoch_time_with_ltz
    }
}

fn register_number_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "to_timestamp",
        |_, domain| {
            int64_domain_to_timestamp_domain(domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_number_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "try_to_timestamp",
        |_, domain| {
            if let Some(domain) = int64_domain_to_timestamp_domain(domain) {
                FunctionDomain::Domain(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                })
            } else {
                FunctionDomain::Full
            }
        },
        error_to_null(eval_number_to_timestamp),
    );

    fn eval_number_to_timestamp(
        val: ValueRef<Int64Type>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<Int64Type, TimestampType>(|val, output, ctx| {
            match int64_to_timestamp(val) {
                Ok(ts) => output.push(ts),
                Err(e) => {
                    ctx.set_error(output.len(), e);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_string_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, DateType, _, _>(
        "to_date",
        |_, _| FunctionDomain::MayThrow,
        eval_string_to_date,
    );
    registry.register_combine_nullable_1_arg::<StringType, DateType, _, _>(
        "try_to_date",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_date),
    );

    fn eval_string_to_date(val: ValueRef<StringType>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<StringType, DateType>(|val, output, ctx| {
            if ctx.func_ctx.enable_strict_datetime_parser {
                match string_to_date(val, ctx.func_ctx.tz.tz, ctx.func_ctx.enable_dst_hour_fix) {
                    Ok(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    Err(e) => {
                        ctx.set_error(output.len(), format!("cannot parse to type `DATE`. {}", e));
                        output.push(0);
                    }
                }
            } else {
                match parse(val) {
                    Ok((naive_dt, _)) => {
                        output.push(naive_dt.date().num_days_from_ce() - EPOCH_DAYS_FROM_CE);
                    }
                    Err(e) => {
                        ctx.set_error(output.len(), format!("cannot parse to type `DATE`. {}", e));
                        output.push(0);
                    }
                }
            }
        })(val, ctx)
    }
}

fn register_timestamp_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_date",
        |ctx, domain| {
            let tz = ctx.tz.tz;
            FunctionDomain::Domain(SimpleDomain {
                min: calc_timestamp_to_date(domain.min, tz),
                max: calc_timestamp_to_date(domain.max, tz),
            })
        },
        eval_timestamp_to_date,
    );
    registry.register_combine_nullable_1_arg::<TimestampType, DateType, _, _>(
        "try_to_date",
        |ctx, domain| {
            let tz = ctx.tz.tz;
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain {
                    min: calc_timestamp_to_date(domain.min, tz),
                    max: calc_timestamp_to_date(domain.max, tz),
                })),
            })
        },
        error_to_null(eval_timestamp_to_date),
    );

    fn eval_timestamp_to_date(
        val: ValueRef<TimestampType>,
        ctx: &mut EvalContext,
    ) -> Value<DateType> {
        vectorize_with_builder_1_arg::<TimestampType, DateType>(|val, output, ctx| {
            let tz = ctx.func_ctx.tz.tz;
            output.push(calc_timestamp_to_date(val, tz));
        })(val, ctx)
    }
    fn calc_timestamp_to_date(val: i64, tz: Tz) -> i32 {
        val.to_timestamp(tz).naive_local().num_days_from_ce() - EPOCH_DAYS_FROM_CE
    }
}

fn register_number_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, DateType, _, _>(
        "to_date",
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast_with_minmax(DATE_MIN, DATE_MAX);
            if overflowing {
                FunctionDomain::MayThrow
            } else {
                FunctionDomain::Domain(domain)
            }
        },
        eval_number_to_date,
    );
    registry.register_combine_nullable_1_arg::<Int64Type, DateType, _, _>(
        "try_to_date",
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast_with_minmax(DATE_MIN, DATE_MAX);
            FunctionDomain::Domain(NullableDomain {
                has_null: overflowing,
                value: Some(Box::new(domain)),
            })
        },
        error_to_null(eval_number_to_date),
    );

    fn eval_number_to_date(val: ValueRef<Int64Type>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<Int64Type, DateType>(|val, output, ctx| {
            match check_date(val) {
                Ok(d) => output.push(d),
                Err(e) => {
                    ctx.set_error(output.len(), e);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_to_string(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_string", &["date_format"]);
    registry.register_combine_nullable_2_arg::<TimestampType, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, StringType, NullableType<StringType>>(
            |date, format, output, ctx| {
                if format.is_empty() {
                    output.push_null();
                } else {
                    let ts = date.to_timestamp(ctx.func_ctx.tz.tz);
                    let res = ts.format(format).to_string();
                    output.push(&res);
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, StringType>(|val, output, ctx| {
            write!(output.data, "{}", date_to_string(val, ctx.func_ctx.tz.tz)).unwrap();
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampType, StringType>(|val, output, ctx| {
            write!(
                output.data,
                "{}",
                timestamp_to_string(val, ctx.func_ctx.tz.tz)
            )
            .unwrap();
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<DateType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<DateType, NullableType<StringType>>(|val, output, ctx| {
            write!(
                output.builder.data,
                "{}",
                date_to_string(val, ctx.func_ctx.tz.tz)
            )
            .unwrap();
            output.builder.commit_row();
            output.validity.push(true);
        }),
    );

    registry.register_combine_nullable_1_arg::<TimestampType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<TimestampType, NullableType<StringType>>(
            |val, output, ctx| {
                write!(
                    output.builder.data,
                    "{}",
                    timestamp_to_string(val, ctx.func_ctx.tz.tz)
                )
                .unwrap();
                output.builder.commit_row();
                output.validity.push(true);
            },
        ),
    );
}

fn register_to_number(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<DateType, NumberType<i64>, _, _>(
        "to_int64",
        |_, domain| FunctionDomain::Domain(domain.overflow_cast().0),
        |val, _| val as i64,
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, NumberType<i64>, _, _>(
        "to_int64",
        |_, domain| FunctionDomain::Domain(*domain),
        |val, _| match val {
            ValueRef::Scalar(scalar) => Value::Scalar(scalar),
            ValueRef::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<DateType, NumberType<i64>, _, _>(
        "try_to_int64",
        |_, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.overflow_cast().0)),
            })
        },
        |val, _| match val {
            ValueRef::Scalar(scalar) => Value::Scalar(Some(scalar as i64)),
            ValueRef::Column(col) => Value::Column(NullableColumn {
                validity: Bitmap::new_constant(true, col.len()),
                column: col.iter().map(|val| *val as i64).collect(),
            }),
        },
    );

    registry.register_combine_nullable_1_arg::<TimestampType, NumberType<i64>, _, _>(
        "try_to_int64",
        |_, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(*domain)),
            })
        },
        |val, _| match val {
            ValueRef::Scalar(scalar) => Value::Scalar(Some(scalar)),
            ValueRef::Column(col) => Value::Column(NullableColumn {
                validity: Bitmap::new_constant(true, col.len()),
                column: col,
            }),
        },
    );
}

macro_rules! signed_ident {
    ($name: ident) => {
        -$name
    };
}

macro_rules! unsigned_ident {
    ($name: ident) => {
        $name
    };
}

macro_rules! impl_register_arith_functions {
    ($name: ident, $op: literal, $signed_wrapper: tt) => {
        fn $name(registry: &mut FunctionRegistry) {
            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_years"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddYearsImpl::eval_date(date, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_years"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddYearsImpl::eval_timestamp(ts, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_quarters"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddMonthsImpl::eval_date(date, ctx.func_ctx.tz, $signed_wrapper!{delta} * 3) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_quarters"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddMonthsImpl::eval_timestamp(ts, ctx.func_ctx.tz, $signed_wrapper!{delta} * 3) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_months"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddMonthsImpl::eval_date(date, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_months"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddMonthsImpl::eval_timestamp(ts, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_days"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddDaysImpl::eval_date(date, $signed_wrapper!{delta}) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_days"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddDaysImpl::eval_timestamp(ts, $signed_wrapper!{delta}) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, TimestampType, _, _>(
                concat!($op, "_hours"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        let val = (ts as i64) * 24 * 3600 * MICROS_IN_A_SEC;
                        match AddTimesImpl::eval_timestamp(
                            val,
                            $signed_wrapper!{delta},
                            FACTOR_HOUR,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_hours"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddTimesImpl::eval_timestamp(
                            ts,
                            $signed_wrapper!{delta},
                            FACTOR_HOUR,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, TimestampType, _, _>(
                concat!($op, "_minutes"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        let val = (ts as i64) * 24 * 3600 * MICROS_IN_A_SEC;

                        match AddTimesImpl::eval_timestamp(
                            val,
                            $signed_wrapper!{delta},
                            FACTOR_MINUTE,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_minutes"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddTimesImpl::eval_timestamp(
                            ts,
                            $signed_wrapper!{delta},
                            FACTOR_MINUTE,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, TimestampType, _, _>(
                concat!($op, "_seconds"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        let val = (ts as i64) * 24 * 3600 * MICROS_IN_A_SEC;

                        match AddTimesImpl::eval_timestamp(
                            val,
                            $signed_wrapper!{delta},
                            FACTOR_SECOND,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_seconds"),

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddTimesImpl::eval_timestamp(
                            ts,
                            $signed_wrapper!{delta},
                            FACTOR_SECOND,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
        }
    };
}

impl_register_arith_functions!(register_add_functions, "add", unsigned_ident);
impl_register_arith_functions!(register_sub_functions, "subtract", signed_ident);

fn register_real_time_functions(registry: &mut FunctionRegistry) {
    registry.register_aliases("now", &["current_timestamp"]);

    registry.properties.insert(
        "now".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "today".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "yesterday".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "tomorrow".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    registry.register_0_arg_core::<TimestampType, _, _>(
        "now",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(ctx.func_ctx.now.timestamp_micros()),
    );

    registry.register_0_arg_core::<DateType, _, _>(
        "today",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(ctx.func_ctx.now, ctx.func_ctx.tz)),
    );

    registry.register_0_arg_core::<DateType, _, _>(
        "yesterday",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(ctx.func_ctx.now, ctx.func_ctx.tz) - 1),
    );

    registry.register_0_arg_core::<DateType, _, _>(
        "tomorrow",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(ctx.func_ctx.now, ctx.func_ctx.tz) + 1),
    );
}

fn register_to_number_functions(registry: &mut FunctionRegistry) {
    // date
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _, _>(
        "to_yyyymm",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMM, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _, _>(
        "to_yyyymmdd",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMMDD, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt64Type, _, _>(
        "to_yyyymmddhh",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt64Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMMDDHH, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt64Type, _, _>(
        "to_yyyymmddhhmmss",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt64Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMMDDHHMMSS, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _, _>(
        "to_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_quarter",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToQuarter, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _, _>(
        "to_day_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToDayOfYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToDayOfMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_day_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToDayOfWeek, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _, _>(
        "to_week_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToWeekOfYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    // timestamp
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _, _>(
        "to_yyyymm",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMM, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _, _>(
        "to_yyyymmdd",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMMDD, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt64Type, _, _>(
        "to_yyyymmddhh",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt64Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMMDDHH, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt64Type, _, _>(
        "to_yyyymmddhhmmss",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt64Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMMDDHHMMSS, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _, _>(
        "to_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_quarter",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToQuarter, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _, _>(
        "to_day_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToDayOfYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToDayOfMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_day_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToDayOfWeek, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _, _>(
        "to_week_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToWeekOfYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, Int64Type, _, _>(
        "to_unix_timestamp",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, Int64Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToUnixTimestamp, _>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_hour",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| ctx.func_ctx.tz.to_hour(val)),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_minute",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| ctx.func_ctx.tz.to_minute(val)),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_second",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| ctx.func_ctx.tz.to_second(val)),
    );
}

fn register_timestamp_add_sub(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "plus",
        |_, lhs, rhs| {
            (|| {
                let lm: i64 = num_traits::cast::cast(lhs.max)?;
                let ln: i64 = num_traits::cast::cast(lhs.min)?;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: check_date(ln + rn).ok()?,
                    max: check_date(lm + rm).ok()?,
                }))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, ctx| {
            match check_date((a as i64) + b) {
                Ok(v) => output.push(v),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            }
        }),
    );

    registry.register_2_arg::<DateType, DateType, Int32Type, _, _>(
        "plus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_add(rn)?,
                    max: lm.checked_add(rm)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a + b,
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "plus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;
                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: check_timestamp(ln + rn).ok()?,
                    max: check_timestamp(lm + rm).ok()?,
                }))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, ctx| match check_timestamp(a + b) {
                Ok(v) => output.push(v),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            },
        ),
    );

    registry.register_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "plus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;
                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_add(rn)?,
                    max: lm.checked_add(rm)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a + b,
    );

    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm: i64 = num_traits::cast::cast(lhs.max)?;
                let ln: i64 = num_traits::cast::cast(lhs.min)?;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: check_date(ln - rn).ok()?,
                    max: check_date(lm - rm).ok()?,
                }))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, ctx| {
            match check_date((a as i64) - b) {
                Ok(v) => output.push(v),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            }
        }),
    );

    registry.register_2_arg::<DateType, DateType, Int32Type, _, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: check_timestamp(ln - rn).ok()?,
                    max: check_timestamp(lm - rm).ok()?,
                }))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, ctx| match check_timestamp(a - b) {
                Ok(v) => output.push(v),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            },
        ),
    );

    registry.register_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Float64Type, _, _>(
        "months_between",
        |_, lhs, rhs| {
            let lm = lhs.max;
            let ln = lhs.min;
            let rm = rhs.max;
            let rn = rhs.min;

            FunctionDomain::Domain(SimpleDomain::<F64> {
                min: months_between(ln, rm).into(),
                max: months_between(lm, rn).into(),
            })
        },
        vectorize_2_arg::<DateType, DateType, Float64Type>(|a, b, _ctx| {
            months_between(a, b).into()
        }),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Float64Type, _, _>(
            "months_between",
            |_, lhs, rhs| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                FunctionDomain::Domain(SimpleDomain::<F64> {
                    min: months_between_ts(ln, rm).into(),
                    max: months_between_ts(lm, rn).into(),
                })
            },
            vectorize_2_arg::<TimestampType, TimestampType, Float64Type>(|a, b, _ctx| {
                months_between_ts(a, b).into()
            }),
        );
}

fn register_rounder_functions(registry: &mut FunctionRegistry) {
    // timestamp -> timestamp
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_second",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Second)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_minute",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Minute)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_five_minutes",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::FiveMinutes)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_ten_minutes",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::TenMinutes)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_fifteen_minutes",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::FifteenMinutes)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_hour",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Hour)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_day",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Day)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "time_slot",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::TimeSlot)
        }),
    );

    // date | timestamp -> date
    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_monday",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToLastMonday>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_monday",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToLastMonday>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToLastSunday>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToLastSunday>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<DateType, Int64Type, DateType>(|val, mode, ctx| {
            if mode == 0 {
                DateRounder::eval_date::<ToLastSunday>(val, ctx.func_ctx.tz)
            } else {
                DateRounder::eval_date::<ToLastMonday>(val, ctx.func_ctx.tz)
            }
        }),
    );
    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<TimestampType, Int64Type, DateType>(|val, mode, ctx| {
            if mode == 0 {
                DateRounder::eval_timestamp::<ToLastSunday>(val, ctx.func_ctx.tz)
            } else {
                DateRounder::eval_timestamp::<ToLastMonday>(val, ctx.func_ctx.tz)
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfMonth>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfMonth>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_quarter",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfQuarter>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_quarter",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfQuarter>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfYear>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfYear>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_iso_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfISOYear>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_iso_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfISOYear>(val, ctx.func_ctx.tz)
        }),
    );
}

// current we don't consider tz here
#[inline]
fn months_between_ts(ts_a: i64, ts_b: i64) -> f64 {
    months_between(
        (ts_a / 86_400_000_000) as i32,
        (ts_b / 86_400_000_000) as i32,
    )
}

#[inline]
fn months_between(date_a: i32, date_b: i32) -> f64 {
    let date_a = Utc
        .timestamp_opt((date_a as i64) * 86400, 0)
        .unwrap()
        .date_naive(); // Assuming date_a is in days
    let date_b = Utc
        .timestamp_opt((date_b as i64) * 86400, 0)
        .unwrap()
        .date_naive(); // Assuming date_b is in days

    let year_diff = date_a.year() - date_b.year();
    let month_diff = date_a.month() as i32 - date_b.month() as i32;

    // Calculate total months difference
    let total_months_diff = year_diff * 12 + month_diff;

    // Determine if special case for fractional part applies
    let is_same_day_of_month = date_a.day() == date_b.day();
    let are_both_end_of_month = date_a
        .checked_add_days(Days::new(1))
        .map(|d| d.month() != date_a.month())
        .unwrap_or(false)
        && date_b
            .checked_add_days(Days::new(1))
            .map(|d| d.month() != date_b.month())
            .unwrap_or(false);

    let day_fraction = if is_same_day_of_month || are_both_end_of_month {
        0.0
    } else {
        let day_diff = date_a.day() as i32 - date_b.day() as i32;
        day_diff as f64 / 31.0 // Using 31-day month for fractional part
    };

    // Total difference including fractional part
    total_months_diff as f64 + day_fraction
}
