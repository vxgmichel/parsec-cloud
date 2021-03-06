from PyQt5.QtWidgets import QWidget

from parsec.core.gui.files_widget import FilesWidget
from parsec.core.gui.workspaces_widget import WorkspacesWidget

from parsec.core.gui.ui.mount_widget import Ui_MountWidget


class MountWidget(QWidget, Ui_MountWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setupUi(self)
        self.files_widget = FilesWidget(parent=self)
        self.workspaces_widget = WorkspacesWidget(parent=self)
        self.layout_content.addWidget(self.files_widget)
        self.layout_content.addWidget(self.workspaces_widget)
        self.files_widget.hide()
        self.workspaces_widget.load_workspace_clicked.connect(self.load_workspace)
        self.files_widget.back_clicked.connect(self.reset)

    def set_core_attrs(self, core, portal):
        self.workspaces_widget.portal = portal
        self.workspaces_widget.core = core
        self.files_widget.portal = portal
        self.files_widget.core = core

    def stop(self):
        self.files_widget.stop()

    def set_mountpoint(self, mountpoint):
        self.files_widget.mountpoint = mountpoint

    def load_workspace(self, workspace_name):
        self.workspaces_widget.hide()
        self.files_widget.set_workspace(workspace_name)
        self.files_widget.show()

    def reset(self):
        self.files_widget.reset()
        self.workspaces_widget.reset()
        self.files_widget.hide()
        self.workspaces_widget.show()
