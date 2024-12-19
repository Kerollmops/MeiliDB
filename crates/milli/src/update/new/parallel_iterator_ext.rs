use std::sync::Arc;

use rayon::iter::ParallelIterator;

pub trait ParallelIteratorExt: ParallelIterator {
    /// A method to run a closure on all the items and return an owned error.
    ///
    /// The init function is ran only as necessary which is basically once per thread.
    fn try_arc_for_each_try_init<F, INIT, T, E>(self, init: INIT, op: F) -> Result<(), E>
    where
        E: Send + Sync,
        F: Fn(&mut T, Self::Item) -> Result<(), Arc<E>> + Sync + Send + Clone,
        INIT: Fn() -> Result<T, E> + Sync + Send + Clone,
    {
        let result = self.try_for_each_init(
            move || match init() {
                Ok(t) => Ok(t),
                Err(err) => Err(Arc::new(err)),
            },
            move |result, item| match result {
                Ok(t) => op(t, item),
                Err(err) => Err(err.clone()),
            },
        );

        match result {
            Ok(()) => Ok(()),
            Err(err) => Err(Arc::into_inner(err).expect("the error must be only owned by us")),
        }
    }
}

impl<T: ParallelIterator> ParallelIteratorExt for T {}
