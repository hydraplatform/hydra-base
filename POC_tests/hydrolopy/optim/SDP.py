#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : SDP, part of hydrology.optim

"""

__all__ = ["sdpSolve", "generateOptimalAllocationTable"]


from numpy import zeros, array, where, arange


def sdpSolve(S1, S2, objp1, AllocObjectives, AllocTable, flow, trProb):
    """Solve one stage of the sochastic dynamic programming problem. The inputs
    are the state at t (S1) and at t+1 (S2), the optimal objectives of the next
    stage (between t+1 and t+2) and the table of optimal allocations for each
    storage transition and flow. The transition probabilites for flow and the
    flows are also needed."""

    objOut = zeros(objp1.shape)
    optAlloc = dict()

    for k in range(len(S1)):  # Iterate over the number of initial storage
                              # intervals.
        TMPobjective = dict()

        for i in range(len(flow)):  # Iterate over the number of current inflow
                                    # intervals.
            for l in range(len(S2)):  # Iterate over the number of final
                                      # storage intervals.
                addObj = 0

                for j in range(len(flow)):  # Iterate over the number of future
                                            # inflow intervals
                    addobjidx = l * 2 + j
                    addObj += trProb[i][j] * objp1[addobjidx]

                TMPobjective.update({(S1[k], S2[l], flow[i]): \
                        AllocObjectives.get((S1[k], S2[l], flow[i])) + addObj})

            objidx = k * 2 + i
            objOut[objidx] = (array(TMPobjective.values())).min()
            minpos = (where(array(TMPobjective.values()) == objOut[objidx]))[0]

            optFinstate = TMPobjective.keys()[minpos][1]
            if AllocTable.get(TMPobjective.keys()[minpos]) != "NF":
                optAlloc.update({(S1[k], flow[i]):
                             AllocTable.get(TMPobjective.keys()[minpos]) + \
                             (optFinstate,)
                             })

    return optAlloc, objOut


def generateOptimalAllocationTable(ResDiscret, flows, releasefun, \
                                   objectivefun):
    """Generate a table of optimal allocations for each pair of initial storage
    and final storage and for each inflow scenario."""

    OptimalAllocTable = dict()
    AllocObjectiveTable = dict()

    for k in range(len(ResDiscret)):
        for l in range(len(ResDiscret)):
            for i in range(len(flows)):
                # Minimise the objective function for all feasible allocations.
                # Since we are only considering discrete flows, the minimum is
                # obtained with a very simple procedure.
                uRange = arange(0, flows[i] + 1, .1)
                tmpAlloc = dict()

                for u in uRange:
                    d = releasefun(ResDiscret[k], ResDiscret[l], u, flows[i])

                    if u >= 0 and d >= 0:
                        tmpAlloc.update({(u, d): objectivefun(u, d)})
                    else:
                        tmpAlloc.update({"NF": objectivefun(0, 0)})

                minObjective = (array(tmpAlloc.values())).min()
                minpos = (where(array(tmpAlloc.values()) == minObjective))[0]
                minAlloc = tmpAlloc.keys()[minpos]

                OptimalAllocTable.update({
                    (ResDiscret[k], ResDiscret[l], flows[i]): minAlloc})
                AllocObjectiveTable.update({
                    (ResDiscret[k], ResDiscret[l], flows[i]): minObjective})

    return AllocObjectiveTable, OptimalAllocTable


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
