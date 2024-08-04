def previous_quarter(exec_date):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import pandas as pd

    target = exec_date - relativedelta(months=3)
    year = target.year
    quarter = pd.Timestamp(target).quarter

    return year, quarter