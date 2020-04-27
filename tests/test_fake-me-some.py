
import unittest
import os
import fake_me_some.fake_me_some as fake_me_some
import fake_me_some.utils as utils
from config_parent import Config


__author__ = "Hung Nguyen"
__copyright__ = "Hung Nguyen"
__license__ = "mit"



#for Reference
         
    # kwargs['out_path'] = os.path.abspath(args.d)
    # kwargs['yaml_file'] = os.path.abspath(args.y)
    # kwargs['out_yaml_file'] = os.path.abspath(args.of)
    # kwargs['out_format'] = os.path.abspath(args.o)
    # kwargs['logging'] = os.path.abspath(args.ll)
    # kwargs['rows'] = os.path.abspath(args.rows)
out_path = '/workspace/tmp/sample_data/'
out_file = '/workspace/tmp/sample_data/test.yaml'
def clean_working_dir(folder: str):
    import os, shutil
     
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)
class Test_fake_me_some(unittest.TestCase,Config):
    def test_01_fake_from_yaml_to_CSV(self): #using environment variables
        os.makedirs(os.path.abspath(self.dirs['working_dir']),exist_ok=True)
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(yaml_file=self.yaml_file,out_path=self.dirs['working_dir'],out_yaml_file=out_file,logging=None,rows=10)
    def test_02_fake_from_yaml_to_DB(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(yaml_file=self.yaml_file,out_format='DB',out_yaml_file=out_file,out_path=self.dirs['working_dir'],logging=None,rows=10)

    def test_03_fake_from_yaml_from_db_to_yaml(self): #using environment variables
        clean_working_dir(self.dirs['working_dir'])
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        db.execute("CREATE schema test")
        os.makedirs(os.path.abspath(self.dirs['working_dir']),exist_ok=True)
        out_file=os.path.join(self.dirs['working_dir'],'new_fake_me_some.yaml')
        fake_me_some.main(yaml_file=self.yaml_file,out_format='DB',out_yaml_file=out_file,out_path=None,logging=None,rows=10)

    def test_04_suggest_some_from_db_to_yaml(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(self.yaml_file)
        
        os.makedirs(os.path.abspath(self.dirs['working_dir']),exist_ok=True)
        out_file=os.path.join(self.dirs['working_dir'],'new_fake_me_some_suggested.yaml')
        fake_me_some.main(yaml_file=self.yaml_file,out_format='SUGGEST',out_yaml_file=out_file,out_path=None,logging=None,rows=10)
        
if __name__ == '__main__':
    unittest.main()