package ServShared

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims defines standard JWT claims for the Servverse ecosystem.
type Claims struct {
	Username string   `json:"username,omitempty"`
	Roles    []string `json:"roles,omitempty"`
	Scopes   []string `json:"scopes,omitempty"`
	TenantID string   `json:"tenant_id,omitempty"`
	jwt.RegisteredClaims
}

type AuthValidator struct {
	jwtSecret     []byte
	jwksURL       string
	oidcIssuer    string
	jwkKeys       map[string]*rsa.PublicKey
	jwkKeysMu     sync.RWMutex
	jwksLastFetch time.Time
}

func NewAuthValidator(secret string, jwksURL string, oidcIssuer string) *AuthValidator {
	return &AuthValidator{
		jwtSecret: []byte(secret),
		jwksURL:   jwksURL,
		oidcIssuer: oidcIssuer,
		jwkKeys:   make(map[string]*rsa.PublicKey),
	}
}

// ValidateToken validates a JWT string and returns its claims.
func (v *AuthValidator) ValidateToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		// Verify signature method
		if v.jwksURL != "" {
			if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			kid, _ := t.Header["kid"].(string)
			if kid == "" {
				return nil, errors.New("missing kid header")
			}
			return v.getRSAPublicKey(kid)
		}

		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return v.jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		if v.oidcIssuer != "" && claims.Issuer != v.oidcIssuer {
			return nil, errors.New("invalid issuer")
		}
		return claims, nil
	}

	return nil, errors.New("invalid token")
}

// ExtractTokenFromHeader parses Authorization Bearer header.
func ExtractTokenFromHeader(headerVal string) (string, error) {
	if headerVal == "" {
		return "", errors.New("authorization header is missing")
	}
	parts := strings.Split(headerVal, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return "", errors.New("authorization header format must be Bearer <token>")
	}
	return parts[1], nil
}

func (v *AuthValidator) getRSAPublicKey(kid string) (*rsa.PublicKey, error) {
	v.jwkKeysMu.RLock()
	key, exists := v.jwkKeys[kid]
	v.jwkKeysMu.RUnlock()

	if exists && time.Since(v.jwksLastFetch) < 1*time.Hour {
		return key, nil
	}

	if err := v.fetchJWKS(); err != nil {
		if exists {
			// Fallback to cached key if fresh fetch fails
			return key, nil
		}
		return nil, err
	}

	v.jwkKeysMu.RLock()
	key, exists = v.jwkKeys[kid]
	v.jwkKeysMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("key id %s not found in JWKS", kid)
	}
	return key, nil
}

type jwkKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwksResponse struct {
	Keys []jwkKey `json:"keys"`
}

func (v *AuthValidator) fetchJWKS() error {
	if v.jwksURL == "" {
		return errors.New("jwks url not configured")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(v.jwksURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch jwks: status code %d", resp.StatusCode)
	}

	var jwks jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return err
	}

	newKeys := make(map[string]*rsa.PublicKey)
	for _, k := range jwks.Keys {
		if k.Kty != "RSA" || k.N == "" || k.E == "" {
			continue
		}

		// Decode modulus N
		nDecoded, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			continue
		}

		// Decode exponent E
		eDecoded, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			continue
		}

		var eVal int
		for _, b := range eDecoded {
			eVal = (eVal << 8) | int(b)
		}

		pubKey := &rsa.PublicKey{
			N: new(big.Int).SetBytes(nDecoded),
			E: eVal,
		}
		newKeys[k.Kid] = pubKey
	}

	v.jwkKeysMu.Lock()
	v.jwkKeys = newKeys
	v.jwksLastFetch = time.Now()
	v.jwkKeysMu.Unlock()

	return nil
}
