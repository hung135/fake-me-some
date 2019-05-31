
import unittest
import os
import fake_me_some.fake_me_some as fake_me_some
from config_parent import Config


__author__ = "Hung Nguyen"
__copyright__ = "Hung Nguyen"
__license__ = "mit"


class Test_fake_me_some(unittest.TestCase,Config):
    def test_01_fake_from_yaml_to_CSV(self): #using environment variables
        os.makedirs(os.path.abspath(self.dirs['working_dir']),exist_ok=True)
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(self.yaml_file,out_path=self.dirs['working_dir'])
    def test_02_fake_from_yaml_to_DB(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(self.yaml_file,'DB',out_path=self.dirs['working_dir'])

    def test_03_fake_from_yaml_from_db_to_yaml(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        db.execute("CREATE schema test")
        os.makedirs(os.path.abspath(self.dirs['working_dir']),exist_ok=True)
        out_file=os.path.join(self.dirs['working_dir'],'new_fake_me_some.yaml')
        fake_me_some.main(self.yaml_file,'DB',out_file)
if __name__ == '__main__':
    unittest.main()