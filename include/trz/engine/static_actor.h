/**
 * @file static_actor.h
 * @brief Compile-time static event dispatch for zero-overhead actor event handling.
 *
 * StaticActor<Derived, Events...> generates a monomorphic dispatch function at
 * compile time and installs it into EventTable::fastDispatch. For single-event
 * actors this eliminates the linear search entirely. For multi-event actors the
 * compiler generates an optimized if-else chain that it can inline and branch-predict.
 *
 * Usage:
 *   class MyActor : public StaticActor<MyActor, EventA, EventB> {
 *   public:
 *       using StaticActor::StaticActor;
 *       void onEvent(const EventA&);
 *       void onEvent(const EventB&);
 *   };
 */

#pragma once

#include "trz/engine/internal/node.h"

namespace tredzone
{

// ---- compile-time dispatch helper -------------------------------------------

namespace detail
{

// Terminal case — no match
template <typename Derived>
inline __attribute__((always_inline)) bool staticDispatch(Derived *, const Actor::Event &)
{
    return false;
}

// Single-event fast path — no branching, direct call
template <typename Derived, typename First>
inline __attribute__((always_inline)) bool staticDispatch(Derived *self, const Actor::Event &event)
{
    (void)event; // eventId check skipped — we know this actor only handles First
    self->onEvent(static_cast<const First &>(event));
    return true;
}

// Multi-event — compile-time if-else chain
template <typename Derived, typename First, typename Second, typename... Rest>
inline __attribute__((always_inline)) bool staticDispatch(Derived *self, const Actor::Event &event)
{
    if (event.getClassId() == Actor::Event::getClassId<First>())
    {
        self->onEvent(static_cast<const First &>(event));
        return true;
    }
    return staticDispatch<Derived, Second, Rest...>(self, event);
}

} // namespace detail

// ---- StaticActor ------------------------------------------------------------

template <typename Derived, typename... HandledEvents>
class StaticActor : public Actor
{
public:
    using Actor::Actor;

    StaticActor()
    {
        registerAll();
        installFastDispatch();
    }

    template <typename Init>
    explicit StaticActor(Init &init) : Actor(init)
    {
        registerAll();
        installFastDispatch();
    }

private:
    void registerAll()
    {
        int dummy[] = {0, (registerOneEvent<HandledEvents>(), 0)...};
        (void)dummy;
    }

    template <typename EventType>
    void registerOneEvent()
    {
        registerEventHandler<EventType>(*static_cast<Derived *>(this));
    }

    // The fast dispatch function — same signature as RegisteredEvent::staticEventHandler.
    // 'handler' is a pointer to the Derived actor (stored in EventTable::fastHandler).
    // No extra indirection: this replaces the linear search + function pointer call
    // with a single function pointer call to a compile-time-resolved handler.
    static bool fastDispatchFn(void *handler, const Actor::Event &event)
    {
        return detail::staticDispatch<Derived, HandledEvents...>(
            static_cast<Derived *>(reinterpret_cast<Actor *>(handler)),
            event);
    }

    void installFastDispatch()
    {
        EventTable *et = getActorId().getEventTable();
        et->fastDispatch = &fastDispatchFn;
        et->fastHandler = static_cast<void *>(static_cast<Actor *>(this));
    }
};

} // namespace tredzone
