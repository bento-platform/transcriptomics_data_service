# Authorization plugin

Although TDS is part of the Bento platform, it is meant to be reusable in other software stacks.
Since authorization requirements and technology vary wildy across different projects, 
TDS allows adopters to write their own authorization logic in python.

For Bento, we rely on API calls to a custom authorization service, 
see [etc/bento.authz.module.py](./etc/bento.authz.module.py) for an example.

For different authorization requirements, you could choose to write a custom module that performs authorization checks based on:
* An API key in the request header or in a cookie
* A JWT bearer token, for example you could:
  * Allow/Deny simply based on the token's validity (decode + TTL)
  * Allow/Deny based on the presence of a scope in the token
  * Allow/Deny based on the presence of a group membership claim
* The results of API calls to an authorization service
* Policy engine evaluations, like OPA or Casbin

## Implementing an authorization plugin

When starting the TDS container, the FastAPI server will attempt to dynamicaly load the authorization plugin 
middleware from `lib/authz.module.py`.

If authorization is enabled and there is no file at `lib/authz.module.py`, an exception will be thrown and the server
will not start.

Furthermore, the content of the file must follow some implementation guidelines:
- You MUST declare a concrete class that extends [BaseAuthzMiddleware](./transcriptomics_data_service/authz/middleware_base.py)
- In that class, you MUST implement the functions from BaseAuthzMiddleware with the expected signatures:
  - `attach`: used to attach the middleware to the FastAPI app.
  - `dipatch`: called for every request made to the API.
  - `dep_authorize_<endpoint>`: endpoint-specific, authz evaluation functions that should return an injectable function.
- Finally, the script should expose an instance of your concrete authz middleware, named `authz_middleware`.

Looking at [bento.authz.module.py](./etc/bento.authz.module.py), we can see an implementation that is specific to 
Bento's authorization service and libraries.

Rather than directly implementing the `attach`, `dispatch` and other authorization logic, we rely on the `bento-lib` 
`FastApiAuthMiddleware`, which already provides a reusable authorization middleware for FastAPI.

The only thing left to do is to implement the endpoint-specific authorization functions.

## Using an authorization plugin

When using the production image, the authz plugin must be mounted correclty on the container.
Assuming you implemented an authz plugin at `~/custom_authz_lib/authz.module.py`, mount the host directory
to the container's `/tds/lib` directory.

```yaml
services:
  tds:
    image: transcriptomics_data_service:latest
    container_name: tds
    # ...
    volumes:
      # Mount the directory containing authz.module.py, NOT the file itself
      - ~/custom_authz_lib:/tds/lib

  tds-db:
    # ... Omitted for simplicity
```

## Providing extra configurations for a custom authorization plugin

You can add custom settings for your authorization plugin.
Following the API key authorization plugin [example](../etc/example.authz.module.py), 
you will notice that the API key is not hard coded in a variable, but imported from the pydantic config.

The TDS pydantic settings are configured to load a `.env` file from the authz plugin mount.
After the .env is loaded, you can access the extra settings with: `config.model_extra.get(<lowercase .env var name>)`.

In other scenarios, you could store any configuration values required for your authorization logic.

## Defining additional python dependencies

When implementing an authorization plugin, you may realize that the default python modules used in TDS are not enough
for your needs.

Maybe you want to use OPA's Python client to evaluate policies, or an in-house Python library your team made for this
purpose.

While the dependencies declared in [pyproject.toml](../pyproject.toml) are fixed for a given TDS release,
you can still speficy extra dependencies to be installed when the container starts!

TODO: figure out how to do this
