from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple, Awaitable
from .models import Event, Handler, Middleware, MiddlewareEntry
import re
import concurrent.futures

class EventBus:
    """
    The EventBus is the main entity of this library.
    It handles events through associated listeners and
    organizes the execution flow.
    """
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Handler]] = {}
        self._static_susbcribers_map: Dict[str, Event] = {}
        self._middlewares: List[MiddlewareEntry] = []
        self._lock = asyncio.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def __del__(self):
        self._executor.shutdown(cancel_futures=True)

    @staticmethod
    def _run_async_in_thread(coro: Awaitable): # NOVO MÉTODO ESTATICO
        """Cria e executa um novo loop de eventos em uma thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    def use(self, middleware: Middleware, topics: list[str] = []) -> None:
        self._middlewares.append(MiddlewareEntry(middleware, topics))
    
    async def subscribe_async(self, topic: str, handler: Callable[[Event], Any], *, priority: int = 0, once: bool = False, max_retries: int = 0) -> str:
        """Same of `subscribe`, but is asynchronous."""
        h = Handler(neg_priority=-priority, callback=handler, once=once, max_retries=max_retries)

        async with self._lock:
            self._subscribers.setdefault(topic, []).append(h)
        
        return h.identifier
    
    def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], Any],
        *,
        priority: int = 0,
        once: bool = False,
        static: bool = False,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        max_retries: int = 0
    ) -> str:
        """Subscribes a listener to an event/topic. When the event is published, the listener is executed."""
        h = Handler(neg_priority=-priority, callback=handler, once=once, static=static, filter_fn=filter_fn, max_retries=max_retries)
        self._subscribers.setdefault(topic, []).append(h)
        self._subscribers[topic].sort(key=lambda h: h.priority, reverse=True)
        
        return h.identifier
    
    async def unsubscribe_async(self, topic: str, handler_id: str) -> bool:
        """Same of `unsubscribe`, but is asynchronous."""
        async with self._lock:
            handlers = self._subscribers.get(topic)

            if not handlers:
                return False

            before = len(handlers)
            handlers[:] = [h for h in handlers if h.identifier != handler_id]

            if not handlers:
                self._subscribers.pop(topic, None)
            
            return len(handlers) != before
        
    def unsubscribe(self, topic: str, handler_id: str) -> bool:
        """Removes a listener from a topic."""
        handlers = self._subscribers.get(topic)

        if not handlers:
            return False

        before = len(handlers)
        handlers[:] = [h for h in handlers if h.identifier != handler_id]

        if not handlers:
            self._subscribers.pop(topic, None)
        
        return len(handlers) != before
    
    def on(
        self,
        topic: str,
        *,
        priority: int = 0,
        once: bool = False,
        static: bool = False,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        max_retries: int = 0
    ) -> Callable[[Callable[[Event], Any]], Callable[[Event], Any]]:
        """Decorator to help creating listeners. It does the same as `subscribe`, but in an easier way."""
        
        def decorator(fn: Callable[[Event], Any]) -> Callable[[Event], Any]:
            self.subscribe(topic, fn, priority=priority, once=once, static=static, filter_fn=filter_fn, max_retries=max_retries)
            return fn
        
        return decorator
            
    async def publish_async(self, topic: str, payload: Any = None, *, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Same of `publish`, but is asynchronous."""
        event = Event(topic=topic, payload=payload, metadata=metadata or {})

        async with self._lock:
            handlers = self._find_handlers(topic)
        
        if not handlers:
            return
        
        async def final_handler(ev: Event):
            to_remove: List[str] = []

            for h in handlers:
                if h.filter_fn and not h.filter_fn(event):
                    continue

                if h.static:
                    if not h.identifier in self._static_susbcribers_map.keys():
                        self._static_susbcribers_map[h.identifier] = ev
                    else:
                        if self._static_susbcribers_map[h.identifier].payload == ev.payload:
                            continue

                await self._execute_handler(h, event)

                if h.once:
                    to_remove.append((topic, h.identifier))
                
            if to_remove:
                async with self._lock:
                    for t, hid in to_remove:
                        hs = self._subscribers.get(t, [])
                        self._subscribers[t] = [x for x in hs if x.identifier != hid]
        
        await self._run_middlewares(event, final_handler)

    def publish(self, topic: str, payload: Any = None, *, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Publishes an event. All the associated listeners will be executed."""
        coro = self.publish_async(topic, payload, metadata=metadata)
        self._executor.submit(EventBus._run_async_in_thread, coro)
    
    def list_subscribers(self) -> Dict[str, List[Tuple[str, int, bool, int]]]:
        return {t: [(h.identifier, h.priority, h.once, h.max_retries) for h in hs] for t, hs in self._subscribers.items()}
    
    def clear(self) -> None:
        self._subscribers.clear()

    @staticmethod
    def _match_topic(pattern: str, topic: str) -> bool:
        regex_pattern = re.escape(pattern)
        regex_pattern = regex_pattern.replace(r"\*", "[^.]+")
        regex_pattern = regex_pattern.replace(r"\#", ".*")
        regex_pattern = f"^{regex_pattern}$"
        return re.match(regex_pattern, topic) is not None
    
    def _find_handlers(self, topic: str) -> List[Handler]:
        handlers: List[Handler] = []
        for pattern, hs in self._subscribers.items():
            if self._match_topic(pattern, topic):
                handlers.extend(hs)
            
        handlers.sort()
        return handlers
    
    async def _execute_handler(self, handler: Handler, event: Event) -> None:
        retries = 0
        backoff_base = 0.2

        while True:
            try:
                if inspect.iscoroutinefunction(handler.callback):
                    await handler.callback(event)
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, handler.callback, event)
                break
            except Exception as e:
                retries += 1
                if retries > handler.max_retries:
                    await self.publish_async('dead.letter', {
                        'original_topic': event.topic,
                        'event_id': event.identifier,
                        'payload': event.payload,
                        'error': str(e),
                    })
                    break
                await asyncio.sleep(backoff_base * (2 ** (retries - 1)))

    async def _run_middlewares(self, event: Event, final_handler: Callable[[Event], Awaitable[None]]):
        """Executa a cadeia de middlewares, terminando no handler final."""
        applicable = [mw.middleware for mw in self._middlewares if mw.matches(event.topic)]
        
        async def _build_chain(index: int) -> Callable[[Event], Awaitable[None]]:
            if index == len(applicable):
                return final_handler
            
            mw = applicable[index]

            async def _next(ev: Event):
                next_in_chain = await _build_chain(index + 1)
                await mw(ev, next_in_chain)
            
            return _next
        
        chain = await _build_chain(0)
        await chain(event)