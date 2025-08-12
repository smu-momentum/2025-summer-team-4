import collections
import time


time_recorder = collections.deque()


def add_time_record(maxsize: int = 10):
    time_recorder.append(time.time())
    while len(time_recorder) > maxsize:
        time_recorder.popleft()


def get_average_time(placeholder: float = 10) -> float:
    if not time_recorder:
        return placeholder
    return (time_recorder[-1]-time_recorder[0]) / len(time_recorder)


def time_string(seconds: float) -> str:
    """초 단위의 시간을 'x시간 x분 x초' 형식으로 변환"""
    if seconds < 60:
        return f'{seconds:.0f}초'
    elif seconds < 3600:
        minutes = int(seconds // 60)
        seconds = int(seconds % 60)
        return f'{minutes}분 {seconds}초'
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        return f'{hours}시간 {minutes}분 {seconds}초'


def memory_string(bytes: int) -> str:
    """바이트 단위의 메모리를 'xMB xKB xB' 형식으로 변환"""
    if bytes < 1024:
        return f'{bytes}B'
    elif bytes < 1024**2:
        kb = bytes / 1024
        return f'{kb:.1f}KB'
    else:
        mb = bytes / (1024**2)
        return f'{mb:.1f}MB'
