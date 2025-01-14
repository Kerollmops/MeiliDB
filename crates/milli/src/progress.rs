use std::any::TypeId;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use serde::Serialize;
use utoipa::ToSchema;

pub trait Step: 'static + Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn current(&self) -> u32;
    fn total(&self) -> u32;
}

#[derive(Clone, Default)]
pub struct Progress {
    steps: Arc<RwLock<Vec<(TypeId, Box<dyn Step>)>>>,
}

impl Progress {
    pub fn update_progress<P: Step>(&self, sub_progress: P) {
        let mut steps = self.steps.write().unwrap();
        let step_type = TypeId::of::<P>();
        if let Some(idx) = steps.iter().position(|(id, _)| *id == step_type) {
            steps.truncate(idx);
        }
        steps.push((step_type, Box::new(sub_progress)));
    }

    // TODO: This code should be in meilisearch_types but cannot because milli can't depend on meilisearch_types
    pub fn as_progress_view(&self) -> ProgressView {
        let steps = self.steps.read().unwrap();

        let mut percentage = 0.0;
        let mut prev_factors = 1.0;

        let mut step_view = Vec::with_capacity(steps.len());
        for (_, step) in steps.iter() {
            prev_factors *= step.total() as f32;
            percentage += step.current() as f32 / prev_factors;

            step_view.push(ProgressStepView {
                current_step: step.name(),
                finished: step.current(),
                total: step.total(),
            });
        }

        ProgressView { steps: step_view, percentage: percentage * 100.0 }
    }
}

/// This trait lets you use the AtomicSubStep defined right below.
/// The name must be a const that never changed but that can't be enforced by the type system because it make the trait non object-safe.
/// By forcing the Default trait + the &'static str we make it harder to miss-use the trait.
pub trait NamedStep: 'static + Send + Sync + Default {
    fn name(&self) -> &'static str;
}

/// Structure to quickly define steps that need very quick, lockless updating of their current step.
/// You can use this struct if:
/// - The name of the step doesn't change
/// - The total number of steps doesn't change
pub struct AtomicSubStep<Name: NamedStep> {
    unit_name: Name,
    current: Arc<AtomicU32>,
    total: u32,
}

impl<Name: NamedStep> AtomicSubStep<Name> {
    pub fn new(total: u32) -> (Arc<AtomicU32>, Self) {
        let current = Arc::new(AtomicU32::new(0));
        (current.clone(), Self { current, total, unit_name: Name::default() })
    }
}

impl<Name: NamedStep> Step for AtomicSubStep<Name> {
    fn name(&self) -> Cow<'static, str> {
        self.unit_name.name().into()
    }

    fn current(&self) -> u32 {
        self.current.load(Ordering::Relaxed)
    }

    fn total(&self) -> u32 {
        self.total
    }
}

#[macro_export]
macro_rules! make_enum_progress {
    ($visibility:vis enum $name:ident { $($variant:ident,)+ }) => {
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Sequence)]
        #[allow(clippy::enum_variant_names)]
        $visibility enum $name {
            $($variant),+
        }

        impl Step for $name {
            fn name(&self) -> Cow<'static, str> {
                use convert_case::Casing;

                match self {
                    $(
                        $name::$variant => stringify!($variant).from_case(convert_case::Case::Camel).to_case(convert_case::Case::Lower).into()
                    ),+
                }
            }

            fn current(&self) -> u32 {
                *self as u32
            }

            fn total(&self) -> u32 {
                Self::CARDINALITY as u32
            }
        }
    };
}

#[macro_export]
macro_rules! make_atomic_progress {
    ($struct_name:ident alias $atomic_struct_name:ident => $step_name:literal) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $struct_name {}
        impl NamedStep for $struct_name {
            fn name(&self) -> &'static str {
                $step_name
            }
        }
        pub type $atomic_struct_name = AtomicSubStep<$struct_name>;
    };
}

make_atomic_progress!(Document alias AtomicDocumentStep => "document" );
make_atomic_progress!(Payload alias AtomicPayloadStep => "payload" );

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ProgressView {
    pub steps: Vec<ProgressStepView>,
    pub percentage: f32,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ProgressStepView {
    pub current_step: Cow<'static, str>,
    pub finished: u32,
    pub total: u32,
}

/// Used when the name can change but it's still the same step.
/// To avoid conflicts on the `TypeId`, create a unique type every time you use this step:
/// ```text
/// enum UpgradeVersion {}    
///
/// progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
///     "v1 to v2",
///     0,
///     10,
/// ));
/// ```
pub struct VariableNameStep<U: Send + Sync + 'static> {
    name: String,
    current: u32,
    total: u32,
    phantom: PhantomData<U>,
}

impl<U: Send + Sync + 'static> VariableNameStep<U> {
    pub fn new(name: impl Into<String>, current: u32, total: u32) -> Self {
        Self { name: name.into(), current, total, phantom: PhantomData }
    }
}

impl<U: Send + Sync + 'static> Step for VariableNameStep<U> {
    fn name(&self) -> Cow<'static, str> {
        self.name.clone().into()
    }

    fn current(&self) -> u32 {
        self.current
    }

    fn total(&self) -> u32 {
        self.total
    }
}
