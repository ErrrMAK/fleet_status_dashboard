"""
Файл для запуска дашборда
"""
import webbrowser
import subprocess
import time
import os

def run_dashboard():
    """
    Запускает Streamlit сервер и открывает браузер
    """
    # Получаем текущую директорию
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Запускаем Streamlit в отдельном процессе
    process = subprocess.Popen(
        ["streamlit", "run", os.path.join(current_dir, "dashboard.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Ждем запуска сервера
    time.sleep(2)
    
    # Открываем браузер
    webbrowser.open("http://localhost:8501")
    
    return process

if __name__ == "__main__":
    run_dashboard() 