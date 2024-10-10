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

use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use tonic::codec::Codec;
use tonic::codec::DecodeBuf;
use tonic::codec::Decoder;
use tonic::codec::EncodeBuf;
use tonic::codec::Encoder;
use tonic::Status;

#[derive(Default)]
pub struct MessageCodec<E, D>(PhantomData<(E, D)>);

impl<E: Message + 'static, D: Message + Default + 'static> Codec for MessageCodec<E, D> {
    type Encode = Arc<E>;
    type Decode = D;
    type Encoder = ArcEncoder<E>;
    type Decoder = DefaultDecoder<D>;

    fn encoder(&mut self) -> Self::Encoder {
        ArcEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        DefaultDecoder(PhantomData)
    }
}

pub struct ArcEncoder<E>(PhantomData<E>);

impl<T: Message> Encoder for ArcEncoder<T> {
    type Item = Arc<T>;

    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.as_ref()
            .encode(dst)
            .map_err(|e| Status::internal(e.to_string()))
    }
}

pub struct DefaultDecoder<D>(PhantomData<D>);

impl<T: Message + Default> Decoder for DefaultDecoder<T> {
    type Item = T;

    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let item = Message::decode(buf)
            .map(Some)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(item)
    }
}
