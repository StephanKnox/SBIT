#from my_sbit_project.workflows.GenericBackfillWkf import BackfillWorkouts
from my_sbit_project.workflows.Backfillwkf import BackfillWkf

class BackfillWkfFactory:
    @staticmethod
    def get_backfill_wkf(wkf_type, backfill_wkf_args) -> BackfillWkf:
 #       if wkf_type == "BackfillWorkouts":
 #           return BackfillWorkouts(**backfill_wkf_args)
 #       else:
 #           raise ValueError("Unknown backfill workflow type")
        pass