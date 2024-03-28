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

use databend_common_base::runtime::metrics::Counter;

#[test]
fn test_clone_counter_metric() {
    let counter = Counter::create(0);
    counter.inc();
    assert_eq!(1, counter.get());

    let counter2 = counter.clone();
    assert_eq!(1, counter2.get());

    counter2.inc_by(2);
    assert_eq!(3, counter.get());
    assert_eq!(3, counter2.get());
}

#[test]
fn test_reset_counter_metric() {
    let counter = Counter::create(0);
    counter.inc();
    assert_eq!(1, counter.get());
    counter.reset();

    let counter2 = counter.clone();
    assert_eq!(0, counter.get());
    assert_eq!(0, counter2.get());

    counter2.inc_by(2);
    assert_eq!(2, counter.get());
    assert_eq!(2, counter2.get());

    counter2.reset();
    assert_eq!(0, counter.get());
    assert_eq!(0, counter2.get());
}
