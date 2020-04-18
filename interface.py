from qtpy.QtWidgets import *
from qtpy.QtCore import Qt
import sys

class Interface(QWidget):

    def __init__(self,Title="application"):
        super().__init__()
        self.setWindowTitle(Title)
        self.setGeometry(300, 300, 250, 150)
        self.initUI()

    def initUI(self):
        self.selectfile()
        # self.buttom()
        # self.textoutput()
        # self.checkbox()
        # self.combobox()

    def selectfile(self):
        global files
        files = QFileDialog.getOpenFileNames(self, 'Open file', '/home')

    def buttom(self):
        bt1=QPushButton("Ok",self)


if __name__=="__main__":
    app = QApplication(sys.argv)
    ex = Interface()
    print(files[0])