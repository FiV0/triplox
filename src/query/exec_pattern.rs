use anyhow::Result;
use edn::query::Variable;

use super::binding_bag::BindingBag;

pub(crate) type ParticipantIndex = usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Proposal {
    proposer: Option<ParticipantIndex>,
    count: usize,
}

impl Proposal {
    pub(crate) fn proposer(&self) -> Option<ParticipantIndex> {
        self.proposer
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn consider(&mut self, proposer: ParticipantIndex, count: usize) {
        if count > 0 && count < self.count {
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

pub(crate) trait ExecPattern: Send + Sync {
    // The variables this pattern participates in.
    fn variables(&self) -> &[Variable];

    // Updates per-row proposals only when this pattern has a strictly cheaper positive count.
    fn count(
        &self,
        input: &BindingBag,
        added: &[Variable],
        participant_index: ParticipantIndex,
        proposals: &mut [Proposal],
    ) -> Result<()>;

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
    fn proposal_keeps_the_first_strictly_cheapest_positive_count() {
        let mut proposal = Proposal::default();

        proposal.consider(4, 0);
        assert_eq!(proposal.proposer(), None);

        proposal.consider(4, 3);
        proposal.consider(5, 3);
        assert_eq!(proposal.proposer(), Some(4));
        assert_eq!(proposal.count(), 3);

        proposal.consider(5, 2);
        assert_eq!(proposal.proposer(), Some(5));
        assert_eq!(proposal.count(), 2);
    }
}
