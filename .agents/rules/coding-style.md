---
trigger: always_on
---

# DistributedJobSchedular Coding Practices & Agent Guidelines

These rules apply strictly to all code generation and architectural choices within the LetsGossip repository.

## 🏗️ Architecture & Design

- **SOLID Principles**: Strictly follow SRP (Single Responsibility), DIP (Dependency Inversion), and OCP (Open-Closed).
- **Domain Independence**: Keep business logic completely independent from infrastructure details (PostgreSQL, Redis, HTTP wrappers, or framework logic).
- **Composition over Inheritance**: Prefer composing behaviors over deep class hierarchies.
- **Dependency Injection**: Keep all dependencies explicit through DI. Avoid global mutable state and static service locators entirely.
- **Extension**: Code should be open for extension but closed for modification. Add new behaviors through new implementations, not by changing stable core logic.

## 💻 Code Quality & Style

- **Small & Focused**: Keep classes and methods small with one clear responsibility.
- **Do not over-engineer**: Avoid unnecessary abstractions, patterns, or complexity. Keep it simple and direct.
- **Meaningful Naming**: Use clear, descriptive names instead of relying on comments.
- **Comments**: Write short, clear comments _only_ where they explain **why**, never _what_ obvious code does.

## 🚨 Concurrency, Errors & Logging

- **Asynchronous I/O**: Use `async/await` for all I/O bound operations.
- **Cancellation Tokens**: Always propagate `CancellationToken` throughout the entire call chain (including Dapper data access).
- **Error Handling**: Handle errors explicitly. **Never silently swallow exceptions.**
- **Logging**: Use structured logging with useful identifiers (e.g., `JobId`, `WorkerId`) to allow for Grafana/Prometheus tracking.

## 🧪 Testing & Maintenance

- **Testability**: Write code that is easy to unit test by isolating external dependencies.
- **Test Coverage**: Add tests for critical business logic, domain state transitions, and concurrency/failure cases.
- **Locality**: Keep changes localized. Do not modify unrelated code while implementing a new feature or fixing a bug.
