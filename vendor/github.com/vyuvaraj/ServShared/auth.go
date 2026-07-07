package ServShared

import (
	"crypto/rsa"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/vyuvaraj/ServShared/pkg/middleware"
)

// Re-expose standard JWT claims.
type Claims = middleware.Claims

type AuthValidator = middleware.AuthValidator

func NewAuthValidator(secret string, jwksURL string, oidcIssuer string) *AuthValidator {
	return middleware.NewAuthValidator(secret, jwksURL, oidcIssuer)
}

func ExtractTokenFromHeader(headerVal string) (string, error) {
	return middleware.ExtractTokenFromHeader(headerVal)
}

// GenerateServiceToken creates a long-lived JWT for inter-service communication.
func GenerateServiceToken(secret string, serviceName string) (string, error) {
	if secret == "" {
		return "", nil // dev mode — no token needed
	}

	claims := Claims{
		Username: serviceName,
		Roles:    []string{"service"},
		Scopes:   []string{"*"},
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "servverse",
			Subject:   serviceName,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(365 * 24 * time.Hour)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// GenerateUserToken creates a JWT for a user with given roles and tenant.
func GenerateUserToken(secret string, username string, roles []string, tenantID string, ttl time.Duration) (string, error) {
	claims := Claims{
		Username: username,
		Roles:    roles,
		TenantID: tenantID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "servverse",
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// GenerateUserTokenRS256 creates a JWT signed with an RSA private key.
func GenerateUserTokenRS256(privKey *rsa.PrivateKey, kid string, username string, roles []string, tenantID string, ttl time.Duration) (string, error) {
	claims := Claims{
		Username: username,
		Roles:    roles,
		TenantID: tenantID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "servverse",
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	return token.SignedString(privKey)
}
