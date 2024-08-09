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

// TODO: Delete this file after fixing the flatc installation issues

extern crate flatbuffers;

#[allow(unused_imports, dead_code)]
pub mod geo_buf {

    use core::cmp::Ordering;
    use core::mem;

    extern crate flatbuffers;
    use self::flatbuffers::EndianScalar;
    use self::flatbuffers::Follow;

    #[deprecated(
        since = "2.0.0",
        note = "Use associated constants instead. This will no longer be generated in 2021."
    )]
    pub const ENUM_MIN_INNER_OBJECT_KIND: u8 = 0;
    #[deprecated(
        since = "2.0.0",
        note = "Use associated constants instead. This will no longer be generated in 2021."
    )]
    pub const ENUM_MAX_INNER_OBJECT_KIND: u8 = 7;
    #[deprecated(
        since = "2.0.0",
        note = "Use associated constants instead. This will no longer be generated in 2021."
    )]
    #[allow(non_camel_case_types)]
    pub const ENUM_VALUES_INNER_OBJECT_KIND: [InnerObjectKind; 8] = [
        InnerObjectKind::Unknown,
        InnerObjectKind::Point,
        InnerObjectKind::LineString,
        InnerObjectKind::Polygon,
        InnerObjectKind::MultiPoint,
        InnerObjectKind::MultiLineString,
        InnerObjectKind::MultiPolygon,
        InnerObjectKind::Collection,
    ];

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    #[repr(transparent)]
    pub struct InnerObjectKind(pub u8);
    #[allow(non_upper_case_globals)]
    impl InnerObjectKind {
        pub const Unknown: Self = Self(0);
        pub const Point: Self = Self(1);
        pub const LineString: Self = Self(2);
        pub const Polygon: Self = Self(3);
        pub const MultiPoint: Self = Self(4);
        pub const MultiLineString: Self = Self(5);
        pub const MultiPolygon: Self = Self(6);
        pub const Collection: Self = Self(7);

        pub const ENUM_MIN: u8 = 0;
        pub const ENUM_MAX: u8 = 7;
        pub const ENUM_VALUES: &'static [Self] = &[
            Self::Unknown,
            Self::Point,
            Self::LineString,
            Self::Polygon,
            Self::MultiPoint,
            Self::MultiLineString,
            Self::MultiPolygon,
            Self::Collection,
        ];
        /// Returns the variant's name or "" if unknown.
        pub fn variant_name(self) -> Option<&'static str> {
            match self {
                Self::Unknown => Some("Unknown"),
                Self::Point => Some("Point"),
                Self::LineString => Some("LineString"),
                Self::Polygon => Some("Polygon"),
                Self::MultiPoint => Some("MultiPoint"),
                Self::MultiLineString => Some("MultiLineString"),
                Self::MultiPolygon => Some("MultiPolygon"),
                Self::Collection => Some("Collection"),
                _ => None,
            }
        }
    }
    impl core::fmt::Debug for InnerObjectKind {
        fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
            if let Some(name) = self.variant_name() {
                f.write_str(name)
            } else {
                f.write_fmt(format_args!("<UNKNOWN {:?}>", self.0))
            }
        }
    }
    impl<'a> flatbuffers::Follow<'a> for InnerObjectKind {
        type Inner = Self;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            let b = flatbuffers::read_scalar_at::<u8>(buf, loc);
            Self(b)
        }
    }

    impl flatbuffers::Push for InnerObjectKind {
        type Output = InnerObjectKind;
        #[inline]
        unsafe fn push(&self, dst: &mut [u8], _written_len: usize) {
            flatbuffers::emplace_scalar::<u8>(dst, self.0);
        }
    }

    impl flatbuffers::EndianScalar for InnerObjectKind {
        type Scalar = u8;
        #[inline]
        fn to_little_endian(self) -> u8 {
            self.0.to_le()
        }
        #[inline]
        #[allow(clippy::wrong_self_convention)]
        fn from_little_endian(v: u8) -> Self {
            let b = u8::from_le(v);
            Self(b)
        }
    }

    impl flatbuffers::Verifiable for InnerObjectKind {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            u8::run_verifier(v, pos)
        }
    }

    impl flatbuffers::SimpleToVerifyInSlice for InnerObjectKind {}
    pub enum InnerObjectOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct InnerObject<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for InnerObject<'a> {
        type Inner = InnerObject<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> InnerObject<'a> {
        pub const VT_WKB_TYPE: flatbuffers::VOffsetT = 4;
        pub const VT_POINT_OFFSETS: flatbuffers::VOffsetT = 6;
        pub const VT_RING_OFFSETS: flatbuffers::VOffsetT = 8;
        pub const VT_COLLECTION: flatbuffers::VOffsetT = 10;
        pub const VT_PROPERTIES: flatbuffers::VOffsetT = 12;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            InnerObject { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<
            'bldr: 'args,
            'args: 'mut_bldr,
            'mut_bldr,
            A: flatbuffers::Allocator + 'bldr,
        >(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            args: &'args InnerObjectArgs<'args>,
        ) -> flatbuffers::WIPOffset<InnerObject<'bldr>> {
            let mut builder = InnerObjectBuilder::new(_fbb);
            if let Some(x) = args.properties {
                builder.add_properties(x);
            }
            if let Some(x) = args.collection {
                builder.add_collection(x);
            }
            if let Some(x) = args.ring_offsets {
                builder.add_ring_offsets(x);
            }
            if let Some(x) = args.point_offsets {
                builder.add_point_offsets(x);
            }
            builder.add_wkb_type(args.wkb_type);
            builder.finish()
        }

        #[inline]
        pub fn wkb_type(&self) -> InnerObjectKind {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<InnerObjectKind>(
                        InnerObject::VT_WKB_TYPE,
                        Some(InnerObjectKind::Unknown),
                    )
                    .unwrap()
            }
        }
        #[inline]
        pub fn point_offsets(&self) -> Option<flatbuffers::Vector<'a, u32>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u32>>>(
                        InnerObject::VT_POINT_OFFSETS,
                        None,
                    )
            }
        }
        #[inline]
        pub fn ring_offsets(&self) -> Option<flatbuffers::Vector<'a, u32>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u32>>>(
                        InnerObject::VT_RING_OFFSETS,
                        None,
                    )
            }
        }
        #[inline]
        pub fn collection(
            &self,
        ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<InnerObject<'a>>>>
        {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab.get::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<InnerObject>>,
                >>(InnerObject::VT_COLLECTION, None)
            }
        }
        #[inline]
        pub fn properties(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        InnerObject::VT_PROPERTIES,
                        None,
                    )
            }
        }
    }

    impl flatbuffers::Verifiable for InnerObject<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<InnerObjectKind>("wkb_type", Self::VT_WKB_TYPE, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u32>>>(
                    "point_offsets",
                    Self::VT_POINT_OFFSETS,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u32>>>(
                    "ring_offsets",
                    Self::VT_RING_OFFSETS,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<InnerObject>>,
                >>("collection", Self::VT_COLLECTION, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "properties",
                    Self::VT_PROPERTIES,
                    false,
                )?
                .finish();
            Ok(())
        }
    }
    pub struct InnerObjectArgs<'a> {
        pub wkb_type: InnerObjectKind,
        pub point_offsets: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u32>>>,
        pub ring_offsets: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u32>>>,
        pub collection: Option<
            flatbuffers::WIPOffset<
                flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<InnerObject<'a>>>,
            >,
        >,
        pub properties: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    }
    impl<'a> Default for InnerObjectArgs<'a> {
        #[inline]
        fn default() -> Self {
            InnerObjectArgs {
                wkb_type: InnerObjectKind::Unknown,
                point_offsets: None,
                ring_offsets: None,
                collection: None,
                properties: None,
            }
        }
    }

    pub struct InnerObjectBuilder<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> InnerObjectBuilder<'a, 'b, A> {
        #[inline]
        pub fn add_wkb_type(&mut self, wkb_type: InnerObjectKind) {
            self.fbb_.push_slot::<InnerObjectKind>(
                InnerObject::VT_WKB_TYPE,
                wkb_type,
                InnerObjectKind::Unknown,
            );
        }
        #[inline]
        pub fn add_point_offsets(
            &mut self,
            point_offsets: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u32>>,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                InnerObject::VT_POINT_OFFSETS,
                point_offsets,
            );
        }
        #[inline]
        pub fn add_ring_offsets(
            &mut self,
            ring_offsets: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u32>>,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                InnerObject::VT_RING_OFFSETS,
                ring_offsets,
            );
        }
        #[inline]
        pub fn add_collection(
            &mut self,
            collection: flatbuffers::WIPOffset<
                flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<InnerObject<'b>>>,
            >,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                InnerObject::VT_COLLECTION,
                collection,
            );
        }
        #[inline]
        pub fn add_properties(
            &mut self,
            properties: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                InnerObject::VT_PROPERTIES,
                properties,
            );
        }
        #[inline]
        pub fn new(
            _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        ) -> InnerObjectBuilder<'a, 'b, A> {
            let start = _fbb.start_table();
            InnerObjectBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<InnerObject<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for InnerObject<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("InnerObject");
            ds.field("wkb_type", &self.wkb_type());
            ds.field("point_offsets", &self.point_offsets());
            ds.field("ring_offsets", &self.ring_offsets());
            ds.field("collection", &self.collection());
            ds.field("properties", &self.properties());
            ds.finish()
        }
    }
    pub enum ObjectOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct Object<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for Object<'a> {
        type Inner = Object<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> Object<'a> {
        pub const VT_SRID: flatbuffers::VOffsetT = 4;
        pub const VT_POINT_OFFSETS: flatbuffers::VOffsetT = 6;
        pub const VT_RING_OFFSETS: flatbuffers::VOffsetT = 8;
        pub const VT_COLLECTION: flatbuffers::VOffsetT = 10;
        pub const VT_PROPERTIES: flatbuffers::VOffsetT = 12;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            Object { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<
            'bldr: 'args,
            'args: 'mut_bldr,
            'mut_bldr,
            A: flatbuffers::Allocator + 'bldr,
        >(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            args: &'args ObjectArgs<'args>,
        ) -> flatbuffers::WIPOffset<Object<'bldr>> {
            let mut builder = ObjectBuilder::new(_fbb);
            if let Some(x) = args.properties {
                builder.add_properties(x);
            }
            if let Some(x) = args.collection {
                builder.add_collection(x);
            }
            if let Some(x) = args.ring_offsets {
                builder.add_ring_offsets(x);
            }
            if let Some(x) = args.point_offsets {
                builder.add_point_offsets(x);
            }
            builder.add_srid(args.srid);
            builder.finish()
        }

        #[inline]
        pub fn srid(&self) -> i32 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe { self._tab.get::<i32>(Object::VT_SRID, Some(0)).unwrap() }
        }
        #[inline]
        pub fn point_offsets(&self) -> Option<flatbuffers::Vector<'a, u32>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u32>>>(
                        Object::VT_POINT_OFFSETS,
                        None,
                    )
            }
        }
        #[inline]
        pub fn ring_offsets(&self) -> Option<flatbuffers::Vector<'a, u32>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u32>>>(
                        Object::VT_RING_OFFSETS,
                        None,
                    )
            }
        }
        #[inline]
        pub fn collection(
            &self,
        ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<InnerObject<'a>>>>
        {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab.get::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<InnerObject>>,
                >>(Object::VT_COLLECTION, None)
            }
        }
        #[inline]
        pub fn properties(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        Object::VT_PROPERTIES,
                        None,
                    )
            }
        }
    }

    impl flatbuffers::Verifiable for Object<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<i32>("srid", Self::VT_SRID, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u32>>>(
                    "point_offsets",
                    Self::VT_POINT_OFFSETS,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u32>>>(
                    "ring_offsets",
                    Self::VT_RING_OFFSETS,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<InnerObject>>,
                >>("collection", Self::VT_COLLECTION, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "properties",
                    Self::VT_PROPERTIES,
                    false,
                )?
                .finish();
            Ok(())
        }
    }
    pub struct ObjectArgs<'a> {
        pub srid: i32,
        pub point_offsets: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u32>>>,
        pub ring_offsets: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u32>>>,
        pub collection: Option<
            flatbuffers::WIPOffset<
                flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<InnerObject<'a>>>,
            >,
        >,
        pub properties: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    }
    impl<'a> Default for ObjectArgs<'a> {
        #[inline]
        fn default() -> Self {
            ObjectArgs {
                srid: 0,
                point_offsets: None,
                ring_offsets: None,
                collection: None,
                properties: None,
            }
        }
    }

    pub struct ObjectBuilder<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> ObjectBuilder<'a, 'b, A> {
        #[inline]
        pub fn add_srid(&mut self, srid: i32) {
            self.fbb_.push_slot::<i32>(Object::VT_SRID, srid, 0);
        }
        #[inline]
        pub fn add_point_offsets(
            &mut self,
            point_offsets: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u32>>,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                Object::VT_POINT_OFFSETS,
                point_offsets,
            );
        }
        #[inline]
        pub fn add_ring_offsets(
            &mut self,
            ring_offsets: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u32>>,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                Object::VT_RING_OFFSETS,
                ring_offsets,
            );
        }
        #[inline]
        pub fn add_collection(
            &mut self,
            collection: flatbuffers::WIPOffset<
                flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<InnerObject<'b>>>,
            >,
        ) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(Object::VT_COLLECTION, collection);
        }
        #[inline]
        pub fn add_properties(
            &mut self,
            properties: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>,
        ) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(Object::VT_PROPERTIES, properties);
        }
        #[inline]
        pub fn new(
            _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        ) -> ObjectBuilder<'a, 'b, A> {
            let start = _fbb.start_table();
            ObjectBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<Object<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for Object<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("Object");
            ds.field("srid", &self.srid());
            ds.field("point_offsets", &self.point_offsets());
            ds.field("ring_offsets", &self.ring_offsets());
            ds.field("collection", &self.collection());
            ds.field("properties", &self.properties());
            ds.finish()
        }
    }
    #[inline]
    /// Verifies that a buffer of bytes contains a `Object`
    /// and returns it.
    /// Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `root_as_object_unchecked`.
    pub fn root_as_object(buf: &[u8]) -> Result<Object, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<Object>(buf)
    }
    #[inline]
    /// Verifies that a buffer of bytes contains a size prefixed
    /// `Object` and returns it.
    /// Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `size_prefixed_root_as_object_unchecked`.
    pub fn size_prefixed_root_as_object(
        buf: &[u8],
    ) -> Result<Object, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::size_prefixed_root::<Object>(buf)
    }
    #[inline]
    /// Verifies, with the given options, that a buffer of bytes
    /// contains a `Object` and returns it.
    /// Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `root_as_object_unchecked`.
    pub fn root_as_object_with_opts<'b, 'o>(
        opts: &'o flatbuffers::VerifierOptions,
        buf: &'b [u8],
    ) -> Result<Object<'b>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root_with_opts::<Object<'b>>(opts, buf)
    }
    #[inline]
    /// Verifies, with the given verifier options, that a buffer of
    /// bytes contains a size prefixed `Object` and returns
    /// it. Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `root_as_object_unchecked`.
    pub fn size_prefixed_root_as_object_with_opts<'b, 'o>(
        opts: &'o flatbuffers::VerifierOptions,
        buf: &'b [u8],
    ) -> Result<Object<'b>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::size_prefixed_root_with_opts::<Object<'b>>(opts, buf)
    }
    #[inline]
    /// Assumes, without verification, that a buffer of bytes contains a Object and returns it.
    /// # Safety
    /// Callers must trust the given bytes do indeed contain a valid `Object`.
    pub unsafe fn root_as_object_unchecked(buf: &[u8]) -> Object {
        flatbuffers::root_unchecked::<Object>(buf)
    }
    #[inline]
    /// Assumes, without verification, that a buffer of bytes contains a size prefixed Object and returns it.
    /// # Safety
    /// Callers must trust the given bytes do indeed contain a valid size prefixed `Object`.
    pub unsafe fn size_prefixed_root_as_object_unchecked(buf: &[u8]) -> Object {
        flatbuffers::size_prefixed_root_unchecked::<Object>(buf)
    }
    #[inline]
    pub fn finish_object_buffer<'a, 'b, A: flatbuffers::Allocator + 'a>(
        fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        root: flatbuffers::WIPOffset<Object<'a>>,
    ) {
        fbb.finish(root, None);
    }

    #[inline]
    pub fn finish_size_prefixed_object_buffer<'a, 'b, A: flatbuffers::Allocator + 'a>(
        fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        root: flatbuffers::WIPOffset<Object<'a>>,
    ) {
        fbb.finish_size_prefixed(root, None);
    }
} // pub mod GeoBuf
