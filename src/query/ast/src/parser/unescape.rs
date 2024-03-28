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

use std::char;
use std::iter::Peekable;

pub fn unescape_string(s: &str, quote: char) -> Option<String> {
    let mut chars = s.chars().peekable();
    let mut s = String::new();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('b') => s.push('\u{0008}'),
                Some('f') => s.push('\u{000C}'),
                Some('n') => s.push('\n'),
                Some('r') => s.push('\r'),
                Some('t') => s.push('\t'),
                Some('\'') => s.push('\''),
                Some('"') => s.push('"'),
                Some('\\') => s.push('\\'),
                Some('u') => s.push(unescape_unicode(&mut chars)?),
                Some('x') => s.push(unescape_byte(&mut chars)?),
                Some(c) if c.is_digit(8) => s.push(unescape_octal(c, &mut chars)),
                Some(c) => {
                    s.push('\\');
                    s.push(c);
                }
                None => {
                    s.push('\\');
                }
            };
        } else if c == quote {
            s.push(quote);
            match chars.next() {
                Some(c) if c != quote => {
                    s.push(c);
                }
                _ => (),
            }
        } else {
            s.push(c);
            continue;
        }
    }

    Some(s)
}

pub fn escape_at_string(s: &str) -> String {
    let chars = s.chars().peekable();
    let mut s = String::new();
    for c in chars {
        match c {
            ' ' => s.push_str("\\ "),
            '\t' => s.push_str("\\\t"),
            '\'' => s.push_str("\\'"),
            '"' => s.push_str("\\\""),
            '\\' => s.push_str("\\\\"),
            _ => s.push(c),
        }
    }
    s
}

pub fn unescape_at_string(s: &str) -> String {
    let mut chars = s.chars().peekable();
    let mut s = String::new();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some(' ') => s.push(' '),
                Some('\t') => s.push('\t'),
                Some('\'') => s.push('\''),
                Some('\"') => s.push('\"'),
                Some('\\') => s.push('\\'),
                Some(c) => {
                    s.push('\\');
                    s.push(c);
                }
                None => {
                    s.push('\\');
                }
            }
        } else {
            s.push(c);
        }
    }

    s
}

fn unescape_unicode(chars: &mut Peekable<impl Iterator<Item = char>>) -> Option<char> {
    let mut code = 0;

    for c in chars.take(4) {
        code = code * 16 + c.to_digit(16)?;
    }

    char::from_u32(code)
}

fn unescape_byte(chars: &mut Peekable<impl Iterator<Item = char>>) -> Option<char> {
    let mut byte = 0;

    for c in chars.take(2) {
        byte = byte * 16 + c.to_digit(16)?;
    }

    char::from_u32(byte)
}

fn unescape_octal(c1: char, chars: &mut Peekable<impl Iterator<Item = char>>) -> char {
    let mut oct = c1.to_digit(8).unwrap();

    while let Some(c) = chars.peek() {
        if let Some(digit) = c.to_digit(8) {
            oct = oct * 8 + digit;
            chars.next();
        } else {
            break;
        }
    }

    char::from_u32(oct).unwrap()
}
