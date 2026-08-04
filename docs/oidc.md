# Verifying Cloud Tasks OIDC tokens locally

When a task carries an `OidcToken`, production Cloud Tasks signs a JWT with
Google's keys and sends it as `Authorization: Bearer <token>`. A Cloud Run
service deployed without `--allow-unauthenticated` — the normal way to run a
task handler — rejects anything whose token does not verify.

That leaves local development with an awkward choice: either skip verification
when running locally (and never exercise the code path that runs in
production), or find an emulator that can produce a token a verifier accepts.

This emulator does the latter. It signs tokens with its own RSA key and
publishes the matching public key, so **your production verification code runs
unchanged locally** — you only point it at a different issuer.

## Start the emulator with an issuer

```bash
docker run --rm -p 8123:8123 -p 8980:8980 \
  ghcr.io/ken109/cloud-tasks-emulator:latest \
  -host 0.0.0.0 -openid-issuer http://localhost:8980
```

`-openid-issuer` does two things:

1. it becomes the `iss` claim of every OIDC token the emulator dispatches, and
2. it tells the emulator which port to serve the discovery endpoints on.

The URL must be one **your handler can reach**, because that is where the
verifier fetches keys from. In Docker Compose that usually means the service
name (`http://cloud-tasks:8980`), not `localhost`.

| Path | Content |
|------|---------|
| `/.well-known/openid-configuration` | OpenID discovery document, pointing at `/jwks` |
| `/jwks` | The public key as a JWK set |
| `/certs` | `{kid: PEM certificate}`, the shape Google serves at `/oauth2/v1/certs` |

Without `-openid-issuer` the tokens are still RS256-signed, but they carry the
production issuer (`https://accounts.google.com`) whose keys the emulator
cannot own — so nothing can verify them. Set the flag whenever you verify.

## Create a task that carries a token

```go
client.CreateTask(ctx, &taskspb.CreateTaskRequest{
    Parent: queueName,
    Task: &taskspb.Task{
        MessageType: &taskspb.Task_HttpRequest{
            HttpRequest: &taskspb.HttpRequest{
                Url:        "http://handler:8080/tasks/process",
                HttpMethod: taskspb.HttpMethod_POST,
                AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
                    OidcToken: &taskspb.OidcToken{
                        ServiceAccountEmail: "tasks@my-project.iam.gserviceaccount.com",
                        // Audience defaults to the target URL, exactly as in
                        // production.
                        Audience: "http://handler:8080",
                    },
                },
            },
        },
    },
})
```

The claims match what production sends: `iss`, `aud`, `sub`, `email`,
`email_verified`, `azp`, `iat`, `exp`.

## Verify it in your handler

The point is that this is the *same* code you deploy. Only the issuer changes,
and that should already be configuration.

### Go

Using [`github.com/coreos/go-oidc`](https://github.com/coreos/go-oidc), which
is what most Go services on Cloud Run use:

```go
// Production: "https://accounts.google.com"
// Local:      "http://cloud-tasks:8980"
issuer := os.Getenv("OIDC_ISSUER")

provider, err := oidc.NewProvider(ctx, issuer)
if err != nil {
    return err
}
verifier := provider.Verifier(&oidc.Config{ClientID: audience})

token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
idToken, err := verifier.Verify(ctx, token)
if err != nil {
    http.Error(w, "unauthorized", http.StatusUnauthorized)
    return
}

var claims struct {
    Email string `json:"email"`
}
if err := idToken.Claims(&claims); err != nil {
    return err
}
```

`oidc.NewProvider` reads `/.well-known/openid-configuration` from the issuer,
follows `jwks_uri` and caches the keys — all of which the emulator serves.

### Python

Using [`PyJWT`](https://pyjwt.readthedocs.io/) with its JWKS client:

```python
import jwt
from jwt import PyJWKClient

issuer = os.environ["OIDC_ISSUER"]          # http://cloud-tasks:8980 locally
jwks = PyJWKClient(f"{issuer}/jwks")

token = request.headers["Authorization"].removeprefix("Bearer ")
key = jwks.get_signing_key_from_jwt(token).key
claims = jwt.decode(token, key, algorithms=["RS256"], audience=audience, issuer=issuer)
```

Google's own `google.oauth2.id_token.verify_oauth2_token` is hard-wired to
Google's certificate URL, so it cannot be pointed at the emulator. Verify with
a generic JWT library and make the issuer configuration — which is better
practice anyway, since it is what makes the path testable at all.

### Node.js

Using [`jose`](https://github.com/panva/jose):

```js
import { createRemoteJWKSet, jwtVerify } from "jose";

const issuer = process.env.OIDC_ISSUER;      // http://cloud-tasks:8980 locally
const jwks = createRemoteJWKSet(new URL(`${issuer}/jwks`));

const token = req.headers.authorization.replace("Bearer ", "");
const { payload } = await jwtVerify(token, jwks, { issuer, audience });
```

## Compose

`-openid-issuer` has to be reachable by the handler, so use the service name:

```yaml
services:
  cloud-tasks:
    image: ghcr.io/ken109/cloud-tasks-emulator:latest
    command:
      - -host=0.0.0.0
      - -openid-issuer=http://cloud-tasks:8980
    ports: ["8123:8123", "8980:8980"]

  handler:
    build: .
    environment:
      # Production sets this to https://accounts.google.com.
      OIDC_ISSUER: http://cloud-tasks:8980
```

## Things to know

- **The signing key is generated per process.** Restarting the emulator rotates
  it. Verifiers that cache JWKS across restarts will fail until the cache
  expires; `go-oidc`, `jose` and `PyJWKClient` all refetch on an unknown `kid`,
  so this rarely bites, but do not pin a key.
- **The token is not signed by Google.** That is the whole point — it is signed
  by the emulator so that you can verify it. Never configure a production
  service to trust the emulator's issuer.
- **`OauthToken` is a placeholder.** Only `OidcToken` produces a verifiable
  token; a task with an `OauthToken` gets an opaque stand-in string.
- The emulator's own test suite verifies a dispatched token against the
  published JWKS and against the published certificate, so this path is checked
  on every commit rather than assumed.
