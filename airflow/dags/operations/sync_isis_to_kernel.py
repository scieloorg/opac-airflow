from datetime import datetime


def parse_date_to_iso_format(date: str) -> str:
    """Traduz datas em formato simples ano-mes-dia, ano-mes para
    o formato iso utilizado durantr a persistência do Kernel"""

    formats = ["%Y-%m-%d", "%Y-%m", "%Y"]

    for format in formats:
        try:
            _date = (
                datetime.strptime(date, format).isoformat(timespec="microseconds") + "Z"
            )
        except ValueError:
            continue
        else:
            return _date

    raise ValueError("Could not parse date '%s' to iso format" % date)
