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

use databend_common_base::runtime::metrics::Gauge;

#[test]
fn test_clone_gauge_metric() {
    let gauge = Gauge::create(0);
    gauge.inc();
    gauge.inc_by(5);
    gauge.dec();
    gauge.dec_by(2);

    let gauge2 = gauge.clone();
    assert_eq!(3, gauge.get());
    assert_eq!(3, gauge2.get());
    gauge2.inc();
    gauge2.inc_by(5);
    gauge2.dec();
    gauge2.dec_by(2);

    assert_eq!(6, gauge.get());
    assert_eq!(6, gauge2.get());

    gauge.set(1);
    assert_eq!(1, gauge.get());
    assert_eq!(1, gauge2.get());
}

// #[test]
// fn test_reset_gauge_metric() {
//     let gauge = Gauge::<i64, AtomicI64>::create(0);
//     gauge.inc();
//     gauge.inc_by(5);
//     gauge.dec();
//     gauge.dec_by(2);
//
//     let gauge2 = gauge.clone();
//     assert_eq!(3, gauge.get());
//     assert_eq!(3, gauge2.get());
//
//     gauge2.reset();
//     gauge2.inc();
//     gauge2.inc_by(5);
//     gauge2.dec();
//     gauge2.dec_by(2);
//
//     assert_eq!(6, gauge.get());
//     assert_eq!(6, gauge2.get());
//
//     gauge.set(1);
//     assert_eq!(1, gauge.get());
//     assert_eq!(1, gauge2.get());
// }
