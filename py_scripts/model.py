from .drv_oracle import Oracle
from .drv_txt import TXT
from .drv_xlsx import XLSX


class Model:
    def __init__(self, table_name, data, output='execute'):
        self.data = data
        self.data['output'] = output
        # self.mode = self.data.get('mode')
        self.driver = {
            'oracle': Oracle,
            'xlsx': XLSX,
            'txt': TXT
        }[self.data.get('source')](table_name, **self.data)

    def init(self):
        return self.driver.init()

    def update(self):
        return self.driver.update()

    def drop(self):
        return self.driver.drop()








