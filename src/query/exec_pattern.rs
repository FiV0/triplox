use anyhow::{bail, Result};
use edn::query::Variable;

use super::binding_bag::BindingBag;

pub(crate) type PatternId = usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Proposal {
    proposer: Option<PatternId>,
    count: usize,
}

impl Proposal {
    pub(crate) fn proposer(&self) -> Option<PatternId> {
        self.proposer
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn consider(&mut self, proposer: PatternId, count: usize) {
        if self.proposer.is_none() || count < self.count {
            self.proposer = Some(proposer);
            self.count = count;
        }
    }
}

impl Default for Proposal {
    fn default() -> Self {
        Self {
            proposer: None,
            count: usize::MAX,
        }
    }
}

// Very much inspired by
// https://github.com/frankmcsherry/datatoad

pub(crate) trait ExecPattern: Send + Sync {
    // The stable identity assigned to this pattern in the executable plan.
    fn id(&self) -> PatternId;

    // The variables this pattern participates in.
    fn variables(&self) -> &[Variable];

    // Updates proposals without a proposer or with a strictly higher count.
    fn count(
        &self,
        _input: &BindingBag,
        _added: &[Variable],
        _proposals: &mut [Proposal],
    ) -> Result<()> {
        bail!("Pattern {} cannot propose", self.id())
    }

    // Extends the `input`` when `added` is non-empty; otherwise filters `input` without changing its layout.
    /// An empty `added` existentially validates the current `input` binding prefix; unbound pattern variables remain for later stages.
    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag>;
}

#[cfg(test)]
mod tests {
    use super::Proposal;

    #[test]
    fn proposal_keeps_the_first_strictly_cheapest_count() {
        let mut proposal = Proposal::default();

        proposal.consider(4, usize::MAX);
        proposal.consider(5, usize::MAX);
        assert_eq!(proposal.proposer(), Some(4));
        assert_eq!(proposal.count(), usize::MAX);

        proposal.consider(5, 3);
        proposal.consider(6, 3);
        assert_eq!(proposal.proposer(), Some(5));
        assert_eq!(proposal.count(), 3);

        proposal.consider(6, 0);
        assert_eq!(proposal.proposer(), Some(6));
        assert_eq!(proposal.count(), 0);
    }
}
