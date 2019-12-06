def shw():
    i=0
    while True:
        i += 1
        time.sleep(3)
        status_update.insert("end",'\r{0}'.format( ['\\', '|', '/', '--'][i % 4]))
        status_update.see("end")

def bar(num):
    progessbar['value']=num
    tk.update_idetask()


if __name__ == "__main__":
    import tkinter as tk,time
    import tkinter.ttk as ttk
    from tkinter import scrolledtext;from threading import Thread
    root = tk.Tk()
    root.title("ICD-自动编码")
    width = root['width'] = 600
    height = root['height'] = 400
    root.resizable(False, False)
    screenwidth = root.winfo_screenwidth()
    screenheight = root.winfo_screenheight()
    alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
    root.geometry(alignstr)
    status = tk.LabelFrame(root,text="Hello World",padx = 10, pady =10)
    status.place(x=20,y=20)
    status_update = scrolledtext.ScrolledText(status,wrap = tk.WORD,width=70,height=20,padx=10,pady=10)
    status_update.grid()
    progessbar = ttk.Progressbar(root,orient="HORIZONTAL",lenght=100,mode="determinate")

    i=0
    p=Thread(target=shw)
    p.setDaemon(True)
    p.start()
    root.mainloop()
