import win32serviceutil
import win32service
import win32event
import time
from logger import setup_logging
from xml_handler import init_xml
from job_runner import check_and_return_jobs

class SchedulerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "SQLSchedulerService"
    _svc_display_name_ = "SQL Scheduler Service"

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        self_running = False

    def SvcDoRun(self):
        setup_logging()
        init_xml()
        while self.running:
            check_and_return_jobs()
            time.sleep(60)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SchedulerService)

