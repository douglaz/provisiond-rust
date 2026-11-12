"""W5: causal interruption traces through real binaries, PG and simulators.

OPS-11/15/21/43/47/49/50, STO-3/7/23/55. Every selector must activate its
named fault; the complete phase refuses missing or duplicate scenario results.
"""

CASES = [
    'w5_admission_before', 'w5_admission_reply', 'w5_claim_reply',
    'w5_step8', 'w5_accepted_lost', 'w5_settlement_blocked',
    'w5_settlement_reply', 'w5_stop_record', 'w5_lock_loss',
    'w5_retained_checkouts', 'w5_process_death', 'w5_connection_check',
    'w5_contention', 'w5_remote_completion', 'w5_generation_record',
]


def w5(h, case=None, defect=None):
    from workflow import Workflow
    w = Workflow(h, defect='generation' if defect == 'w5_generation' else defect)
    try:
        for name in ([case] if case else CASES):
            w.case = name
            w.stop_engine()
            raise NotImplementedError(name)
    finally:
        w.close()
