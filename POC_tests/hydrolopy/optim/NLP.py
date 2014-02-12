#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : NLP, part of hydrology.optim

"""

__all__ = ["one_reservoir_NLP"]

import coopr.pyomo as pyomo

def one_reservoir_NLP(Smin, Smax, Rmax, hmin, hmax, eff, Th, initS, \
                      targetS, price, evap, qin, Vofh_coeff, \
                      deltat=86400, rho=1000, g=9.81):
    """This function generates a single reservoir non-linear programming model
    instance, as it is used by the pyomo solvers. 
    
    It is recommended to use the Ipopt solver provided by `COIN-OR
    <https://projects.coin-or.org/Ipopt>`_. 
    
    ================= =========================================================
    Input parameters
    ================= =========================================================
    *Smin*            Maximum storage of the reservoir
    *Smax*            Minimum storage
    *Rmax*            Maximum release. The minimum release is considered to be
                      zero
    *hmin*            Minimum water level in m a.s.l.
    *hmax*            Maximum water level in m a.s.l.
    *eff*             Turbine efficiency (value between 0 and 1)
    *Th*              Turbine elevation in m a.s.l.
    *initS*           Initial storage.
    *targetS*         Target storage at the end of the optimization period.
    *price*           Time series of the energy price.
    *evap*            Time series of evaporation.
    *qin*             Time series of reservoir inflow.
    *Vofh_coeff*      Coefficients of the quadratic function used to
                      approximate the relationship between water level and
                      storage (volume).
    *deltat*=86400    Time step in seconds, defaults to one day.
    *rho*=1000        Density of water. Default: 1000 kg m^-3
    *g*=9.81          Acceleration of gravity. Default: 9.81 m s^-2
    ================= =========================================================
    """

    model = pyomo.ConcreteModel()

#---- Time axis  Attention: The time axis here is only used during the forecast
#                           horizon. Parameters, which in any case are constant
#                           throughout the forecast period, should be defined
#                           as constant parameters.
    model.maxT = pyomo.Param(within=pyomo.NonNegativeIntegers, \
                             initialize=len(qin))
    model.t = pyomo.RangeSet(1, model.maxT)

#---- Parameters
#------ Time independent
    model.rho = pyomo.Param(within=pyomo.Reals, \
                            initialize=rho)  # density of water
    model.g = pyomo.Param(within=pyomo.Reals, \
                          initialize=g)  # earth's acceleration

    model.eff = pyomo.Param(within=pyomo.PercentFraction, \
                            initialize=eff)  # Turbine efficiency
    model.Th = pyomo.Param(within=pyomo.NonNegativeReals, \
                           initialize=Th)  # Elevation of the turbines
    model.deltat = pyomo.Param(within=pyomo.NonNegativeReals, \
                               initialize=deltat)  # Time step

    model.Smin = pyomo.Param(within=pyomo.NonNegativeReals, initialize=Smin)
    model.Smax = pyomo.Param(within=pyomo.NonNegativeReals, initialize=Smax)

    model.Rmax = pyomo.Param(within=pyomo.NonNegativeReals, initialize=Rmax)

    model.hmin = pyomo.Param(within=pyomo.NonNegativeReals, initialize=hmin)
    model.hmax = pyomo.Param(within=pyomo.NonNegativeReals, initialize=hmax)

    model.initS = pyomo.Param(within=pyomo.NonNegativeReals, \
                              initialize=float(initS))  # initial storage
    model.targetS = pyomo.Param(within=pyomo.NonNegativeReals, \
                                initialize=float(targetS))  # target storage

    # Polynomial approximation of the relation between head
    # and storage

    Vofh = np.poly1d(Vofh_coeff)
    #hofV = lambda x: (-Vofh_coeff[1] + \
    #                  (Vofh_coeff[1] ** 2 \
    #                   - 4 * Vofh_coeff[0] * (Vofh_coeff[2] - x)) ** .5) \
    #                  / (2 * Vofh_coeff[0])

#------ Time dependent
    P_init = {}
    n = 1
    for p in price:
        P_init[n] = p
        n += 1

    model.P = pyomo.Param(model.t, initialize=P_init)  # Energy price
#    model.Evap = pyomo.Param(model.t, initialize=evap)  # Evaporation

    q_init = {}
    n = 1
    for q in qin:
        q_init[n] = q
        n += 1

    model.q = pyomo.Param(model.t, initialize=q_init)  # Inflow

#---- Initial values of the decision variables

    Rinit = {}
    hinit = {}
    Sinit = {}
    for t in model.t:
        Rinit[t] = Rmax
        hinit[t] = hmax
        Sinit[t] = Smax

#---- Variables

    model.R = pyomo.Var(model.t, initialize=Rinit, \
                        domain=pyomo.NonNegativeReals)  # Release
    model.h = pyomo.Var(model.t, initialize=hinit, \
                        domain=pyomo.NonNegativeReals)  # Water level
    model.S = pyomo.Var(model.t, initialize=Sinit, \
                        domain=pyomo.NonNegativeReals)  # Storage

#---- Objective function

    def objective_function(model):
        return model.rho * model.g * model.deltat * model.eff \
               * sum([(model.h[t] - model.Th) * model.R[t] \
               * model.P[t] for t in model.t])
               #* sum(model.R[t] * model.P[t] for t in model.t)
               #* pyomo.summation(pyomo.summation(model.h, model.R), model.P)

    model.OBJ = pyomo.Objective(rule=objective_function, sense=pyomo.maximize)

#---- Constraints

#------ Mass balance
    #def mass_balance(model, t):
    #    if t == 1:
    #        return Vofh(model.h[t]) == model.initS + \
    #                               (model.q[t] - model.R[t]) * model.deltat
    #    else:
    #        return Vofh(model.h[t]) == Vofh(model.h[t-1]) + \
    #                                   (model.q[t] - model.R[t]) \
    #                                   * model.deltat

    def mass_balance(model, t):
        if t == 1:
            return model.S[t] == model.initS + (model.q[t] - model.R[t]) \
                                 * model.deltat
        else:
            return model.S[t] == model.S[t - 1] + (model.q[t] - model.R[t]) \
                                 * model.deltat

#------ Initial storage
    #def initial_storage(model, t):
    #    if t == 1:
    #        #return model.S[t] == model.initS
    #        return Vofh(model.h[t]) == model.initS
    #    else:
    #        return pyomo.Constraint.Feasible

#------ Target storage
    def target_storage(model, t):
        if t == model.maxT:
            return model.S[t] == model.targetS
            #return Vofh(model.h[t]) == model.targetS
        else:
            return pyomo.Constraint.Feasible

#------ Relation between model.h and model.S
    def hS_relation(model, t):
        if t == 1:
            return (model.initS + model.S[t]) / 2 == Vofh(model.h[t])
        else:
            return (model.S[t - 1] + model.S[t]) / 2 == Vofh(model.h[t])

#    def Sh_relation(model, t):
#        return model.h[t] == hofV(model.S[t])

#------ Range of release, storage and head
    def R_range(model, t):
        return model.R[t] <= model.Rmax

    def S_range(model, t):
        return (model.Smin, model.S[t], model.Smax)

    def h_range(model, t):
        return (model.hmin, model.h[t], model.hmax)

    model.mass_balance = pyomo.Constraint(model.t, rule=mass_balance)
    model.target_storage = pyomo.Constraint(model.t, rule=target_storage)
    #model.initial_storage = pyomo.Constraint(model.t, rule= initial_storage)
    model.hS_relation = pyomo.Constraint(model.t, rule=hS_relation)
    #model.Sh_relation = pyomo.Constraint(model.t, rule=Sh_relation)

    model.R_range = pyomo.Constraint(model.t, rule=R_range)
    model.S_range = pyomo.Constraint(model.t, rule=S_range)
    model.h_range = pyomo.Constraint(model.t, rule=h_range)

    return model


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
