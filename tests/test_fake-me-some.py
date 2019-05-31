
import unittest
 
import fake_me_some.fake_me_some as fake_me_some


__author__ = "Hung Nguyen"
__copyright__ = "Hung Nguyen"
__license__ = "mit"

yaml_file='/workspace/tests/test_config.yaml'
class Test_fake_me_some(unittest.TestCase):
    def test_fake_from_yaml_to_CSV(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(yaml_file)
    def test_fake_from_yaml_to_DB(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(yaml_file,'DB')

    def test_fake_from_yaml_from_db_to_yaml(self): #using environment variables
        yaml,db=fake_me_some.pre_process_yaml(yaml_file)
        db.execute("CREATE schema test")
        fake_me_some.main(yaml_file,'DB','new_fake_me_some.yaml')
if __name__ == '__main__':
    unittest.main()