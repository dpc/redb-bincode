use std::borrow::Borrow;
use std::fmt;

pub trait SortOrder {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering;
}

#[derive(Debug)]
pub struct Lexicographical;

impl SortOrder for Lexicographical {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

#[derive(Debug)]
pub struct SortKey<T>(pub T);

impl<T> Borrow<T> for SortKey<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

impl<T> redb::Value for SortKey<T>
where
    T: SortOrder + fmt::Debug,
{
    type SelfType<'a> = &'a [u8] where Self: 'a;

    type AsBytes<'a> = &'a [u8] where Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn type_name() -> redb::TypeName {
        <&[u8] as redb::Value>::type_name()
    }
}

impl<T> redb::Key for SortKey<T>
where
    T: SortOrder + fmt::Debug,
{
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        <T as SortOrder>::compare(data1, data2)
    }
}
