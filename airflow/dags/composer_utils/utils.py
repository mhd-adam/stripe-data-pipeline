from airflow.utils.context import Context


def error_callback_func(context: Context):
    '''
    Implement and use this method to send error message to dedicated Slack channel defined in Slack connection with some context.
    '''
    pass
