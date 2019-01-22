import time, os


def benchmark():
    start = time.time()

    for num in range(2, 1000000):
        for count in range(2, int(num ** 0.5) + 1):
            if not num % count:
                # print("Nope")
                break
        else:
            # print("Yup")
            pass
    
    end = time.time()
    """time_taken = end - start
    cpu_count = os.cpu_count()
    adjusted_score = cpu_count / time_taken * 100

    print("Time taken =", time_taken)
    print("Cpu cores =", cpu_count)
    print("Adjusted compute score =", adjusted_score)"""

    adjusted_score = os.cpu_count() / (end - start) * 100

    return adjusted_score

print(benchmark())


