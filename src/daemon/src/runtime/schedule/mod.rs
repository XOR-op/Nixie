pub(super) mod policy;
pub(super) mod statistics;

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
