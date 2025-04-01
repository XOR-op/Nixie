mod policy;
mod scheduler;
mod statistics;

pub use scheduler::Scheduler;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Priority {
    Dynamic { level: PriorityLevel, weight: i8 },
    Fixed(PriorityLevel),
}

impl Priority {
    pub fn level(&self) -> PriorityLevel {
        match self {
            Priority::Dynamic { level, .. } => *level,
            Priority::Fixed(level) => *level,
        }
    }

    pub const fn default_dynamic() -> Self {
        Self::Dynamic {
            level: PriorityLevel::Interactive,
            weight: 0,
        }
    }

    pub fn increase(&mut self, until: Option<PriorityLevel>) -> bool {
        match self {
            Priority::Dynamic { level, weight } => {
                let next = level.to_u8().saturating_add(1);
                if next > until.unwrap_or(PriorityLevel::max()).to_u8() {
                    return false;
                }
                *self = Priority::Dynamic {
                    level: PriorityLevel::from(next),
                    weight: *weight,
                };
                true
            }
            Priority::Fixed(_) => false,
        }
    }

    pub fn decrease(&mut self, until: Option<PriorityLevel>) -> bool {
        match self {
            Priority::Dynamic { level, weight } => {
                let next = level.to_u8().saturating_sub(1);
                if next == level.to_u8() || next < until.unwrap_or(PriorityLevel::min()).to_u8() {
                    return false;
                }
                *self = Priority::Dynamic {
                    level: PriorityLevel::from(next),
                    weight: *weight,
                };
                true
            }
            Priority::Fixed(_) => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PriorityLevel {
    Interactive,
    LowInteractive,
    Batch,
    Background,
}

impl PriorityLevel {
    fn to_u8(&self) -> u8 {
        match self {
            PriorityLevel::Interactive => 3,
            PriorityLevel::LowInteractive => 2,
            PriorityLevel::Batch => 1,
            PriorityLevel::Background => 0,
        }
    }

    pub const fn max() -> Self {
        PriorityLevel::Interactive
    }

    pub const fn min() -> Self {
        PriorityLevel::Background
    }
}

// from u8
impl From<u8> for PriorityLevel {
    fn from(val: u8) -> Self {
        match val {
            3 => PriorityLevel::Interactive,
            2 => PriorityLevel::LowInteractive,
            1 => PriorityLevel::Batch,
            0 => PriorityLevel::Background,
            _ => panic!("Invalid priority level"),
        }
    }
}

impl PartialOrd for PriorityLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.to_u8().partial_cmp(&other.to_u8())
    }
}

impl Ord for PriorityLevel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_u8().cmp(&other.to_u8())
    }
}
