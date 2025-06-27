class RecoveryAreaUsage:
    def __init__(self, File_Type, Percent_Space_Used, Percent_Space_Reclaimable):
        self.File_Type = File_Type
        self.Percent_Space_Used = Percent_Space_Used 
        self.Percent_Space_Reclaimable = Percent_Space_Reclaimable
        

    def get_file_type(self):
        return self.file_type

    def get_percent_space_used(self):
        return self.percent_space_used

    def get_Percent_Space_Reclaimable(self):
        return self.percent_space_reclaimable