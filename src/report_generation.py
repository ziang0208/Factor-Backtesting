import matplotlib.pyplot as plt
import numpy as np

class ReportGenerator:
    def __init__(self, result, pnl, pro, ic):
        self.result = result
        self.pnl = pnl
        self.pro = pro
        self.ic = ic

    def generate_report(self):
        for factor in self.ic:
            ic = np.mean(self.ic[factor]).round(3)
            if ic > 0:
                self.pro[factor].columns = self.pro[factor].columns[::-1]
                self.pnl[factor].columns = self.pnl[factor].columns[::-1]

            df = self.pro[factor]
            cumulative_returns = (1 + df).cumprod()
            dates = df.index
            plt.figure(figsize=(12, 6))
            for group in cumulative_returns.columns:
                plt.plot(dates, cumulative_returns[group], label=f'group_{group}')
            plt.title(factor + '_pro')
            plt.xlabel('date')
            plt.ylabel('pro')
            plt.legend()
            plt.grid(True)
            x_ticks_interval = 30
            plt.xticks(dates[::x_ticks_interval], rotation=45)
            plt.show()

            self.pnl[factor]['hedge'] = self.pnl[factor][1] - self.pnl[factor][10]
            df = self.pnl[factor]
            cumulative_returns = (1 + df).cumprod()
            dates = df.index
            plt.figure(figsize=(12, 6))
            for group in cumulative_returns.columns:
                plt.plot(dates, cumulative_returns[group], label=f'group_{group}')
            plt.title(factor + '_pnl')
            plt.xlabel('date')
            plt.ylabel('pnl')
            plt.legend()
            plt.grid(True)
            x_ticks_interval = 30
            plt.xticks(dates[::x_ticks_interval], rotation=45)
            plt.show()

        return self.result