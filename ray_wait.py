import ray


@ray.remote
def test_f(wid):
    return f"call {wid} done"


@ray.remote
class FailActor:

    def __init__(self):
        self.status = 0

    def fail(self):
        self.status += 1
        raise Exception("Oops..")

    def success(self):
        return self.status

    def terminate(self):
        ray.actor.exit_actor()


def print_cpus():
    print(ray.available_resources().get('CPU', 0))


if __name__ == "__main__":
    ray.init()
    ids = list()
    for i in range(5):
        ids.append(test_f.remote(i))
    ready, waiting = ray.wait(ids, 5)
    print(ready)
    print(waiting)
    ready, waiting = ray.wait(ids, 1)
    print(ready)
    print(waiting)

    print_cpus()
    actor = FailActor.remote()

    print_cpus()
    try:
        print(ray.get(actor.fail.remote()))
    except Exception as e:
        print(e)
        #actor.__ray_kill__()
        ray.wait([actor.terminate.remote()])

    print_cpus()
    actor = FailActor.remote()
    print(ray.get(actor.success.remote()))