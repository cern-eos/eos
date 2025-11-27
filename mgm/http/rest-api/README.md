# EOS MGM REST API - Implementation Overview

This document describes how the EOS MGM REST API is implemented in this codebase and how to extend it.

## High-level architecture

- Router-centric dispatch
  - `Router` performs URL pattern + HTTP method matching and invokes a bound handler callable.
  - URL patterns support path placeholders (e.g. `/api/v1/stage/{id}`) via the shared `URLParser`.

- Handlers
  - `RestHandler` is the minimal base: validates the entrypoint (e.g. `/api/`), exposes `isRestRequest`, and holds the entry URL.
  - `TapeRestHandler` registers routes for the Tape API (stage, archiveinfo, release) and delegates to Action objects.
  - `WellKnownHandler` serves the discovery endpoint (`/.well-known/wlcg-tape-rest-api`) and is implemented via an inlined route.

- Actions (business-facing request handlers)
  - Small classes deriving from `Action` or `TapeAction`, each implementing `run(HttpRequest*, VirtualIdentity*)`.
  - Stage API actions: Create, Get, Cancel, Delete.
  - ArchiveInfo and Release actions.
  - Actions call the business layer (`TapeRestApiBusiness`) and translate errors to HTTP responses.

- Business layer
  - `TapeRestApiBusiness` encapsulates calls to MGM subsystems for stage/evict/query operations and throws typed exceptions when needed.

- Responses
  - `RestApiResponse<T>` and `RestApiResponseFactory` remain as the generic envelope.
  - `RestResponseFactory` is the unified, high-level factory used across handlers/actions (e.g. Ok, Created, BadRequest, NotFound, Forbidden, MethodNotAllowed, NotImplemented, InternalError).

- Exceptions
  - `exception/Exceptions.hh` is an umbrella header exposing common REST exceptions: NotFound, MethodNotAllowed, Forbidden, NotImplemented, ObjectNotFound, ActionNotFound, ControllerNotFound, etc.
  - `JsonValidationException` is header-only and carries validation details for 400 responses.
  - Tape business exceptions are also included via the umbrella.

- Models and JSON
  - Models live under `model/...`; JSON builders and jsonifiers live under `json/...`.
  - `JsonCppModelBuilder` provides JSON parsing with consistent error reporting; concrete model builders validate and construct typed models.
  - Jsonifiers convert models to JSON for responses.

- Configuration
  - `TapeRestApiConfig` configures access URL, sitename, host alias, ports, activation flags, and optional version-to-endpoint mappings.
  - `Constants.hh` holds small shared constants (e.g. `URLPARAM_ID`).

## Request flow

1. `RestApiManager` chooses the handler based on the request URL prefix (e.g. Tape API vs `/.well-known`).
2. The selected `RestHandler` (e.g. `TapeRestHandler`) uses the `Router` to dispatch to a bound action or inline route.
3. The action validates/reads input (model builder), calls the business layer, and returns a response via `RestResponseFactory`.
4. Errors are mapped centrally by `HandleWithErrors` (see `response/ErrorHandling.hh`) or locally via `RestResponseFactory` helpers.

## Adding a new endpoint

1. Choose the API version and base path (e.g. `/api/v1/custom/`).
2. In the relevant handler (most likely `TapeRestHandler::initialize*Routes`):
   - Create an `Action` (or bind a small lambda) that implements the logic.
   - Register a route with `mRouter.add("/api/v1/custom/...", HttpHandler::Methods::<VERB>, handler)`. Use `URLPARAM_ID` placeholders as needed.
3. If input JSON is required, create a model + builder under `model/...` and `json/.../model-builders`, then instantiate/validate in the action.
4. Build responses using `RestResponseFactory` (e.g. `Ok(model)`, `Created(model, headers)`, `BadRequest(ex)`, `NotFound()`, `InternalError(msg)`).
5. Add or update unit tests under `unit_tests/mgm/http/rest-api/tape` (or a new suite) to cover routing, validation, and responses.

## Error handling guidelines

- Use `JsonValidationException` for bad inputs (400).
- Use `NotFoundException`/`ObjectNotFoundException` to return 404.
- Use `MethodNotAllowedException` to return 405.
- Use `ForbiddenException` for 403.
- Use `NotImplementedException` for 501 when a version/feature is declared but inactive.
- For unexpected failures, map to `InternalError` (500) with a concise message.

## Notes

- The legacy controller/factory layers were removed in favor of direct routing.
- The `.well-known` endpoint is implemented inline within `WellKnownHandler`.
- Many small exception headers were consolidated under `exception/Exceptions.hh`.
- Response factories were unified under `response/RestResponseFactory{.hh,.cc}`.

This structure aims to keep the code easy to navigate, reduce file count, and make adding endpoints straightforward, while preserving clear separation between routing, request handling, business logic, and JSON serialization.
