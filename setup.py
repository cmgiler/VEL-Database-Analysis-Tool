from cx_Freeze import setup, Executable

setup(name='VEL_Convert',
      version='0.1',
      description='Parse VEL Report and save to JSON File',
      executables = [Executable('run.py')])