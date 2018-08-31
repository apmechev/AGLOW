

AGLOW Operators
~~~~~~~~~~~~~~~~~~~~~
Derived from the airflow base operator class, the AGLOW operators integrate with staging services and the GRID\_LRT package. These sensors are used to define, package, build and launch LOFAR jobs.

LRT_Sandbox
------
This operator builds and uploads a sandbox, given a sandbox definition file. As an input, it takes a configuration file and returns the sandbox location in a dictionary with a key "SBX_location".


.. autoclass:: AGLOW.airflow.operators.LRT_Sandbox.LRTSandboxOperator
    :members: 
    :undoc-members:

