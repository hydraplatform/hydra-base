To help with identifying features, This is a list of "user stories" which I hope will help give context, focus development, and identify features to prioritise in order to create an MVP.

Users:
======

1. Xavier: The administrator. Has all global permissions.
2. Alice: Manager, Organisation A
3. Adam: Modeller in Team A, Organisation A
4. Anna: Modeller in Team A, Organisation A
4. Albert: Modeller in Team B, Organisation A
4. Aria: Trainee in Team B, Organisation A
5. Claire: Consultant -- can be in any organisation

Use Cases:
==========

Organisations:
1. Organisation A and Organisation B call Xavier, requesting their own secure organisation accounts on Hydra.
2. Xavier creates 2 organisations: `Organisation A` and `Organisation B`
3. Xavier creates 2 users: `Alice` and `Barbara`
4. Xavier places `Alice` in `Organisation A` and `Barbara` in `Organisation B`
5. Xavier makes Alice and Barbara admins of their respective organisations.
6. Alice and Barbara can now add projects, users and groups to their organisations.

Simple
------
`Alice` creates 2 new teams: : `Adam` and `Albert`. These are added to the `Organisation A` group only.

 *Alice* creates a project, called 'Additional Analysis'. She creates a new team called 'Modellers', placing
 Adam and Albert to the group. Alice cannot see or add Barbara.
 Alice shares her project with the Modellers group. Adam and Albert can now see the project.

 Managing User Sub-Groups
 ------------------------
`Alice` requires `Adam` to perform a modelling task. She creates a


Claire is in Team A and Team B but NOT in Organisation A.

Claire creates a project and wants to share it with members of Team B.
The list of available users should be: Albert, Aria and Alice, but NOT Adam and Anna.
