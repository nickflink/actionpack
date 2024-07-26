import asyncio
import functools
import inspect
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from functools import reduce
from itertools import islice
from itertools import tee
from typing import Generic
from typing import Iterator
from typing import Iterable
from typing import Tuple

from actionpack.action import Name
from actionpack.action import Outcome
from actionpack.action import Result
from actionpack import Action

import logging

# TODO(nick.flink) clean this up
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

class Procedure(Generic[Name, Outcome]):

    def __init__(self, actions: Iterable[Action[Name, Outcome]]):
        if not (isinstance(actions, Iterator) or isinstance(actions, Iterable)):
            raise TypeError(f'Actions must be iterable. Received {type(actions).__name__}.')

        self.actions, self._actions, self.__actions = tee(actions, 3)

    def validate(self):
        actions, spare = tee(self.__actions, 2)
        self.__actions = spare
        for action in actions:
            if not isinstance(action, Action):
                msg = f'Procedures can only execute Actions: {str(action)}'
                raise Procedure.NotAnAction(msg)
        return self

    async def aio_gen(
        self,
        should_raise: bool = False
    ) -> Iterator[Result[Outcome]]:
        for action in self.actions:
            logger.debug(f"running action {action}")
            if inspect.iscoroutinefunction(action.aperform):
                ret = await action.aperform(should_raise=should_raise) if should_raise else await action.aperform()
            else:
                loop = asyncio.get_running_loop()
                ret = await loop.run_in_executor(None, action.perform, {
    'should_raise': should_raise})
            yield ret

    async def aio_execute(
        self,
        should_raise: bool = False
    ) -> Iterator[Result[Outcome]]:
        val = [a async for a in self.aio_gen(should_raise)]
        logger.debug(f"aio_execute {val}")
        return val

    def execute(
        self,
        max_workers: int = 5,
        should_raise: bool = False,
        synchronously: bool = True
    ) -> Iterator[Result[Outcome]]:
        actions, spare = tee(self._actions, 2)
        self._actions = spare
        if synchronously:
            for action in actions:
                yield action.perform(should_raise=should_raise) if should_raise else action.perform()
        elif max_workers <= 0:
            logger.debug("running asyncio for Procedure")
            for t in asyncio.run(self.aio_execute(should_raise)):
                yield t
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(action._perform, should_raise=should_raise): str(action) for action in actions}
                for future in as_completed(futures):
                    yield future.result()
        return True

    def __repr__(self):
        actions, spare = tee(self.actions, 2)
        self.actions = spare
        limit = 5
        some_actions = list(islice(actions, limit))
        try:
            next(actions)
            more_actions = True
        except StopIteration:
            more_actions = False
        header = '\nProcedure for performing the following Actions:\n'
        tab = '  '
        bullet = f'{tab}* '
        action_list = reduce(lambda a, b: str(a) + f'\n{bullet}' + str(b), some_actions)
        footer = f'\n{tab}...\n' if more_actions else ''
        return header + bullet + str(action_list) + footer

    def __iter__(self):
        return self._actions

    def __next__(self):
        try:
            return next(self._actions)
        except StopIteration:
            return None

    class NotAnAction(Exception):
        pass


class KeyedProcedure(Procedure[Name, Outcome]):

    def __init__(self, actions: Iterable[Action[Name, Outcome]]):
        if not (isinstance(actions, Iterator) or isinstance(actions, Iterable)):
            raise TypeError(f'Actions must be iterable. Received {type(actions)}.')

        self.actions, self._actions, self.__actions = tee(actions, 3)

    def validate(self):
        actions, spare = tee(self.__actions, 2)
        self.__actions = spare
        for action in actions:
            if not isinstance(action, Action):
                msg = f'Procedures can only execute Actions: {str(action)}'
                raise Procedure.NotAnAction(msg)
            if action.name is None:
                msg = f'All {self.__class__.__name__} Actions must have a name: {str(action)}'
                raise KeyedProcedure.UnnamedAction(msg)
        return self

    async def aio_gen(
        self,
        should_raise: bool = False
    ) -> Iterator[Result[Outcome]]:
        for action in self.actions:
            ret = await action.aperform(should_raise=should_raise)
            yield (action.name, ret)

    async def aio_execute(
        self,
        should_raise: bool = False
    ) -> Iterator[Result[Outcome]]:
        val = [a async for a in self.aio_gen(should_raise)]
        logger.debug(f"aio_execute {val}")
        return val

    def execute(
        self,
        max_workers: int = 5,
        should_raise: bool = False,
        synchronously: bool = True
    ) -> Iterator[Tuple[Name, Result[Outcome]]]:
        if synchronously:
            for action in self:
                yield (action.name, action.perform(should_raise=should_raise)) \
                      if should_raise else (action.name, action.perform())
        elif max_workers <= 0:
            logger.debug("running asyncio for KeyedProcedure")
            for t in asyncio.run(self.aio_execute(should_raise)):
                yield t
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(action._perform, should_raise=should_raise): action for action in self}
                for future in as_completed(futures):
                    yield (futures[future].name, future.result())

    class UnnamedAction(Exception):
        pass
