package org.dancres.paxos.impl;

import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Claim;
import org.dancres.paxos.messages.Collect;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**

 RECOVERY
 ========

 Suspend collect validation (Deny, Allow), just apply Then: aka if rnd > current_rnd store & record on disk


 */
public class Protocol {
    /**
     * Tests against the low watermark
     */
    interface CheckSeq<T> extends BiFunction<T, Watermark, Boolean> {
    }

    static CheckSeq<Claim> isOldSeq = (c, w) -> c.getSeqNum() < w.getSeqNum() + 1;
    static CheckSeq<Begin> shouldWriteBegin = (b, w) -> b.getSeqNum() > w.getSeqNum();

    /**
     * Tests against the last elected Collect
     *
     * @param <T>
     */
    interface CheckRnd<T> extends BiFunction<T, Collect, Boolean> {
    }

    static CheckRnd<Claim> isPastRnd = (c, e) -> c.getRndNumber() < e.getRndNumber();
    static CheckRnd<Collect> isElectableRnd = (c, e) -> c.getRndNumber() > e.getRndNumber();
    static CheckRnd<Begin> isTheElectedRnd = (b, e) -> b.getRndNumber() == e.getRndNumber();
    static CheckRnd<Collect> shouldWriteCollect = isElectableRnd;

    /**
     * Test for expiry
     */
    static Function<Long, Boolean> isExpired = l -> System.currentTimeMillis() > l;

    /**
     * Test for same InetSocketAddress
     */
    static BiFunction<InetSocketAddress, InetSocketAddress, Boolean> isSameSource = InetSocketAddress::equals;

    static class StateMachine {
        private static final InetSocketAddress INITIAL_ADDR =
                new InetSocketAddress(Utils.getWorkableInterfaceAddress(), 12345);
        private Collect _elected = Collect.INITIAL;
        private InetSocketAddress _elector = INITIAL_ADDR;
        private long _expiry = 0;

        /**
         * COLLECT
         * =======
         * <p>
         * Old:
         * <p>
         * rnd < current rnd => emit OldRound
         * <p>
         * seq < low_water + 1
         * <p>
         * Actionable:
         * <p>
         * If source is same and round >= current rnd (equal because it's possible leader will retry due to dropped packets)
         * - DO WE NEED LEGITIMATELY TO WORRY ABOUT = ? IF WE DO IT WOULD BE BECAUSE LEADER IS GOING TO RETRY THE COLLECT DUE TO PACKET LOSS BUT SHOULD IT? AND IF IT DOES, SHOULD IT NOT JUST INCREMENT ROUND?
         * - IT SEEMS THE ANSWER IS NO, WE SHOULD REMOVE THIS - A LEADER NOT SEEING ALL LASTS WILL TIMEOUT AND RETRY AT WHICH POINT IT WOULD RECEIVE OLD_ROUNDS AND EXIT CAUSING RESYNC OF ROUND_NUM. WHAT
         * WE'RE CURRENTLY DOING IS SAVING SOME CYCLES BUT IT SHOULDN'T BE NECESSARY - TEST THIS TOMORROW BY REMOVING THE = FROM LEADERUTILS.SAMELEADER ROUND CHECK IN A FRESH CLONE OF THE MASTER
         * - IN FACT WE'VE NEVER SEEN MULTI-PAXOS MESSAGE, THERE SEEMS TO BE A BUG IN AMACCEPTING() TEST. FOR A DIFFERENT LEADER WITH A NEW ROUND IT SHOULD IGNORE UNLESS OLD LEADER'S LEASE HAS EXPIRED.
         * IN EFFECT IT SHOULD ONLY BLOCK NEW ROUNDS FROM LEADERS FOR THE DURATION OF THE LEASE. OTHERWISE THE USUAL LOGIC APPLIES WHICH MEANS THE LEADER WILL BE REJECTED BECAUSE IT'S USING AN ALREADY AGREED TO
         * RND.
         * - WE WILL LIKELY NEED TO RE-WIRE SOME TESTS TO FEED THE AL THE CORRECT NEXT ROUND NUMBER ABOVE LONG.MIN_VALUE AS IS CURRENTLY SET IN COLLECT.INITIAL SO WE'D NEED COLLECT.INITIAL.RNDNUMBER + 1 WHICH IS
         * EFFECTIVELY WHAT PROPOSAL ALLOCATOR DOES FOR LEGITIMATE/NON-TEST LEADER ACTIONS ON FIRST BOOT.
         * <p>
         * If source is not same source and round > current rnd and leader lease expired
         * <p>
         * Then:
         * <p>
         * If rnd > current_rnd store & record on disk
         * <p>
         * Emit LAST for seqNum
         * <p>
         * NOTE: ABOVE RULES MEAN WE DON'T OLD ROUND FOR A NEW RND FROM A NEW LEADER INSIDE THE LEADER LEASE, WE'RE SILENT AS INTENDED.
         */

        void dispatch (Collect aProposed, InetSocketAddress aProposer, Watermark aWatermark,
                       Outdated anOld, Act anAction) {
            dispatch(aProposed, aProposer, aWatermark, anOld, anAction, false);
        }

        void dispatch(Collect aProposed, InetSocketAddress aProposer, Watermark aWatermark,
                      Outdated anOld, Act anAction, boolean isRecovery) {

            if ((!isRecovery) && (old(aProposed, _elected, aWatermark))) {
                anOld.accept(_elected.getRndNumber(), _elector);
            } else if ((isRecovery) || (actionable(aProposed, aProposer, _elected, _elector, _expiry))) {
                Collect myPreviouslyElected = _elected;
                _elected = aProposed;
                _elector = aProposer;

                extendExpiry();
                anAction.accept(aProposed, isRecovery || shouldWriteCollect.apply(aProposed, myPreviouslyElected));
            }
        }

        /**
         * BEGIN
         * =====
         * <p>
         * Old:
         * <p>
         * rnd < current rnd => OldRound
         * <p>
         * seq < low_water + 1
         * <p>
         * Actionable:
         * <p>
         * If rnd match current collect
         * <p>
         * Then:
         * <p>
         * If seqNum > low_watermark store & record on disk
         * <p>
         * Emit accept for seqNum
         */
        void dispatch(Begin aBegin, Watermark aLowWatermark, Outdated anOld, Act anAction) {
            dispatch(aBegin, aLowWatermark, anOld, anAction, false);
        }

        void dispatch(Begin aBegin, Watermark aLowWatermark, Outdated anOld, Act anAction, boolean isRecovery) {
            if ((!isRecovery) && (old(aBegin, _elected, aLowWatermark))) {
                anOld.accept(_elected.getRndNumber(), _elector);
            } else if ((isRecovery) || (actionable(aBegin, _elected))) {
                extendExpiry();
                anAction.accept(aBegin, isRecovery || shouldWriteBegin.apply(aBegin, aLowWatermark));
            }
        }

        Collect getElected() {
            return _elected;
        }

        InetSocketAddress getElector() {
            return _elector;
        }

        void resetElected() {
            _elected = Collect.INITIAL;
            _elector = INITIAL_ADDR;

        }
        void extendExpiry() {
            _expiry = System.currentTimeMillis() + Leader.LeaseDuration.get();
        }

        long getExpiry() {
            return _expiry;
        }
        
        /**
         * Reject a Collect if it is past (old round), not electable (hence round not greater than current)
         * or for an old sequence number.
         *
         * @param aProposedCollect
         * @param anElectedCollect
         * @param aLowWatermark
         * @return
         */
        boolean old(Collect aProposedCollect, Collect anElectedCollect, Watermark aLowWatermark) {
            return (isPastRnd.apply(aProposedCollect, anElectedCollect) ||
                    (! isElectableRnd.apply(aProposedCollect, anElectedCollect)) ||
                    (isOldSeq.apply(aProposedCollect, aLowWatermark)));
        }

        /**
         * A Collect will be actioned if it is electable (newer round) and either the source is as previously or
         * because the leader lease has expired (hence any source is fine).
         *
         * @param aProposedCollect
         * @param aProposerSource
         * @param anElectedCollect
         * @param anElectedSource
         * @param anElectedExpiry
         * @return
         */
        boolean actionable(Collect aProposedCollect, InetSocketAddress aProposerSource,
                                  Collect anElectedCollect, InetSocketAddress anElectedSource,
                                  long anElectedExpiry) {
            return (isElectableRnd.apply(aProposedCollect, anElectedCollect)) &&
                    (isSameSource.apply(aProposerSource, anElectedSource) ||
                            (isExpired.apply(anElectedExpiry)));
        }

        /**
         * Reject a Begin if it is past (old round) or for an old sequence number
         *
         * @param aProposedBegin
         * @param anElectedCollect
         * @param aLowWatermark
         * @return
         */
        boolean old(Begin aProposedBegin, Collect anElectedCollect, Watermark aLowWatermark) {
            return ((isPastRnd.apply(aProposedBegin, anElectedCollect)) ||
                    (isOldSeq.apply(aProposedBegin, aLowWatermark)));
        }

        /**
         * A Begin will be actioned if it comes from the currently elected leader. If it doesn't and it's not past
         * or old, we should not action and remain quiet because the related collect hasn't been seen and we are
         * behind ledger actions.
         * 
         * @param aProposedBegin
         * @param anElectedCollect
         * @return
         */
        boolean actionable(Begin aProposedBegin, Collect anElectedCollect) {
            return isTheElectedRnd.apply(aProposedBegin, anElectedCollect);
        }
    }

    /**
     * Report that a Claim has been made against an old round using the currently elected round and the elector
     */
    interface Outdated extends BiConsumer<Long, InetSocketAddress> {
    }

    /**
     * Act on the provided Claim and log it if directed
     */
    interface Act extends BiConsumer<Claim, Boolean> {
    }
}
