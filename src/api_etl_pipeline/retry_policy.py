from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


class RetryableHttpError(RuntimeError):
    pass


def with_retry(max_attempts: int = 3):
    return retry(
        reraise=True,
        retry=retry_if_exception_type(RetryableHttpError),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(initial=0.2, max=5),
    )
