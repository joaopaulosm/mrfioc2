# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'evgsoftseq.ui'
#
# Created: Thu Mar 24 14:40:26 2011
#      by: PyQt4 UI code generator 4.7.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_EvgSoftSeq(object):
    def setupUi(self, EvgSoftSeq):
        EvgSoftSeq.setObjectName("EvgSoftSeq")
        EvgSoftSeq.resize(304, 392)
        self.centralWidget = QtGui.QWidget(EvgSoftSeq)
        self.centralWidget.setObjectName("centralWidget")
        self.heading_label = QtGui.QLabel(self.centralWidget)
        self.heading_label.setGeometry(QtCore.QRect(9, 9, 181, 17))
        font = QtGui.QFont()
        font.setPointSize(14)
        font.setWeight(75)
        font.setBold(True)
        self.heading_label.setFont(font)
        self.heading_label.setObjectName("heading_label")
        self.tableWidget = QtGui.QTableWidget(self.centralWidget)
        self.tableWidget.setGeometry(QtCore.QRect(9, 32, 256, 192))
        self.tableWidget.setRowCount(2047)
        self.tableWidget.setColumnCount(2)
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(2)
        self.tableWidget.setRowCount(2047)
        self.pb_setSoftSeq = QtGui.QPushButton(self.centralWidget)
        self.pb_setSoftSeq.setGeometry(QtCore.QRect(10, 310, 85, 27))
        self.pb_setSoftSeq.setObjectName("pb_setSoftSeq")
        self.rb_tsConvert = QtGui.QRadioButton(self.centralWidget)
        self.rb_tsConvert.setGeometry(QtCore.QRect(10, 250, 141, 19))
        self.rb_tsConvert.setChecked(True)
        self.rb_tsConvert.setObjectName("rb_tsConvert")
        self.rb_tsRaw = QtGui.QRadioButton(self.centralWidget)
        self.rb_tsRaw.setGeometry(QtCore.QRect(150, 250, 151, 19))
        self.rb_tsRaw.setObjectName("rb_tsRaw")
        self.Label_timestampUnit = QtGui.QLabel(self.centralWidget)
        self.Label_timestampUnit.setGeometry(QtCore.QRect(10, 230, 171, 16))
        font = QtGui.QFont()
        font.setPointSize(12)
        font.setWeight(75)
        font.setBold(True)
        self.Label_timestampUnit.setFont(font)
        self.Label_timestampUnit.setObjectName("Label_timestampUnit")
        self.label_tsRes = QtGui.QLabel(self.centralWidget)
        self.label_tsRes.setGeometry(QtCore.QRect(80, 280, 171, 16))
        font = QtGui.QFont()
        font.setPointSize(12)
        self.label_tsRes.setFont(font)
        self.label_tsRes.setObjectName("label_tsRes")
        self.Label_tsRes = QtGui.QLabel(self.centralWidget)
        self.Label_tsRes.setGeometry(QtCore.QRect(20, 280, 61, 16))
        font = QtGui.QFont()
        font.setPointSize(12)
        font.setWeight(75)
        font.setBold(True)
        self.Label_tsRes.setFont(font)
        self.Label_tsRes.setObjectName("Label_tsRes")
        EvgSoftSeq.setCentralWidget(self.centralWidget)
        self.menuBar = QtGui.QMenuBar(EvgSoftSeq)
        self.menuBar.setGeometry(QtCore.QRect(0, 0, 304, 21))
        self.menuBar.setObjectName("menuBar")
        EvgSoftSeq.setMenuBar(self.menuBar)
        self.mainToolBar = QtGui.QToolBar(EvgSoftSeq)
        self.mainToolBar.setObjectName("mainToolBar")
        EvgSoftSeq.addToolBar(QtCore.Qt.ToolBarArea(QtCore.Qt.TopToolBarArea), self.mainToolBar)
        self.statusBar = QtGui.QStatusBar(EvgSoftSeq)
        self.statusBar.setObjectName("statusBar")
        EvgSoftSeq.setStatusBar(self.statusBar)
        self.toolBar = QtGui.QToolBar(EvgSoftSeq)
        self.toolBar.setObjectName("toolBar")
        EvgSoftSeq.addToolBar(QtCore.Qt.ToolBarArea(QtCore.Qt.TopToolBarArea), self.toolBar)

        self.retranslateUi(EvgSoftSeq)
        QtCore.QMetaObject.connectSlotsByName(EvgSoftSeq)

    def retranslateUi(self, EvgSoftSeq):
        EvgSoftSeq.setWindowTitle(QtGui.QApplication.translate("EvgSoftSeq", "EvgSoftSeq", None, QtGui.QApplication.UnicodeUTF8))
        self.heading_label.setText(QtGui.QApplication.translate("EvgSoftSeq", "TextLabel", None, QtGui.QApplication.UnicodeUTF8))
        self.pb_setSoftSeq.setText(QtGui.QApplication.translate("EvgSoftSeq", "Set", None, QtGui.QApplication.UnicodeUTF8))
        self.rb_tsConvert.setText(QtGui.QApplication.translate("EvgSoftSeq", "Sub-Second (EGU)", None, QtGui.QApplication.UnicodeUTF8))
        self.rb_tsRaw.setText(QtGui.QApplication.translate("EvgSoftSeq", "Event Clock Ticks (TICKS)", None, QtGui.QApplication.UnicodeUTF8))
        self.Label_timestampUnit.setText(QtGui.QApplication.translate("EvgSoftSeq", "Timestamp Input Mode", None, QtGui.QApplication.UnicodeUTF8))
        self.label_tsRes.setText(QtGui.QApplication.translate("EvgSoftSeq", "TextLabel", None, QtGui.QApplication.UnicodeUTF8))
        self.Label_tsRes.setText(QtGui.QApplication.translate("EvgSoftSeq", "1 Unit = ", None, QtGui.QApplication.UnicodeUTF8))
        self.toolBar.setWindowTitle(QtGui.QApplication.translate("EvgSoftSeq", "toolBar", None, QtGui.QApplication.UnicodeUTF8))

