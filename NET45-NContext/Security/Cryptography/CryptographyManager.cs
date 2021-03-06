﻿namespace NContext.Security.Cryptography
{
    using System;
    using System.ComponentModel.Composition;
    using System.Security.Cryptography;

    using NContext.Configuration;

    /// <summary>
    /// Defines manager class for application cryptography-related operations.
    /// </summary>
    /// <remarks></remarks>
    // TODO: (DG) Add support for default symmetric keys.
    public class CryptographyManager : IManageCryptography
    {
        private readonly CryptographyConfiguration _CryptographyConfiguration;

        private Type _DefaultHashAlgorithm;

        private Type _DefaultKeyedHashAlgorithm;

        private Type _DefaultSymmetricAlgorithm;

        private Lazy<IProvideHashing> _HashProvider;

        private Lazy<IProvideKeyedHashing> _KeyedHashProvider;

        private Lazy<IProvideSymmetricEncryption> _SymmetricEncryptionProvider;

        private Boolean _IsConfigured;

        /// <summary>
        /// Initializes a new instance of the <see cref="CryptographyManager"/> class.
        /// </summary>
        /// <param name="cryptographyConfiguration">The cryptography configuration.</param>
        /// <remarks></remarks>
        public CryptographyManager(CryptographyConfiguration cryptographyConfiguration)
        {
            if (cryptographyConfiguration == null)
            {
                throw new ArgumentNullException("cryptographyConfiguration");
            }

            _CryptographyConfiguration = cryptographyConfiguration;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is configured.
        /// </summary>
        /// <remarks></remarks>
        public Boolean IsConfigured
        {
            get
            {
                return _IsConfigured;
            }
        }

        /// <summary>
        /// Gets or sets the default symmetric algorithm. Default is <see cref="AesManaged"/>.
        /// </summary>
        /// <value>The default symmetric algorithm.</value>
        /// <remarks></remarks>
        public Type DefaultSymmetricAlgorithm
        {
            get
            {
                return _DefaultSymmetricAlgorithm ?? typeof(AesManaged);
            }
            set
            {
                _DefaultSymmetricAlgorithm = value;
            }
        }

        /// <summary>
        /// Gets or sets the default keyed hash algorithm. Default is <see cref="HMACSHA256"/>.
        /// </summary>
        /// <value>The default keyed hash algorithm.</value>
        /// <remarks></remarks>
        public Type DefaultKeyedHashAlgorithm
        {
            get
            {
                return _DefaultKeyedHashAlgorithm ?? typeof(HMACSHA256);
            }
            set
            {
                _DefaultKeyedHashAlgorithm = value;
            }
        }

        /// <summary>
        /// Gets the default hash algorithm. Default is <see cref="SHA256Managed"/>.
        /// </summary>
        /// <remarks></remarks>
        public Type DefaultHashAlgorithm
        {
            get
            {
                return _DefaultHashAlgorithm ?? typeof(SHA256Managed);
            }
            set
            {
                _DefaultHashAlgorithm = value;
            }
        }

        /// <summary>
        /// Gets the hash provider.
        /// </summary>
        /// <remarks></remarks>
        public IProvideHashing HashProvider
        {
            get
            {
                return _HashProvider.Value;
            }
        }

        /// <summary>
        /// Gets the keyed hash provider.
        /// </summary>
        /// <remarks></remarks>
        public IProvideKeyedHashing KeyedHashProvider
        {
            get
            {
                return _KeyedHashProvider.Value;
            }
        }

        /// <summary>
        /// Gets the symmetric encryption provider.
        /// </summary>
        /// <remarks></remarks>
        public IProvideSymmetricEncryption SymmetricEncryptionProvider
        {
            get
            {
                return _SymmetricEncryptionProvider.Value;
            }
        }

        /// <summary>
        /// Configures the component instance.
        /// </summary>
        /// <param name="applicationConfiguration">The application configuration.</param>
        /// <remarks></remarks>
        public void Configure(ApplicationConfigurationBase applicationConfiguration)
        {
            if (!_IsConfigured)
            {
                applicationConfiguration.CompositionContainer.ComposeExportedValue<IManageCryptography>(this);

                _DefaultHashAlgorithm = _CryptographyConfiguration.DefaultHashAlgorithm;
                _DefaultKeyedHashAlgorithm = _CryptographyConfiguration.DefaultKeyedHashAlgorithm;
                _DefaultSymmetricAlgorithm = _CryptographyConfiguration.DefaultSymmetricAlgorithm;

                _HashProvider = new Lazy<IProvideHashing>(
                    _CryptographyConfiguration.HashProviderFactory 
                        ?? (() => new HashProvider(DefaultHashAlgorithm)));

                _KeyedHashProvider = new Lazy<IProvideKeyedHashing>(
                    _CryptographyConfiguration.KeyedHashProviderFactory 
                        ?? (() => new KeyedHashProvider(DefaultKeyedHashAlgorithm)));

                _SymmetricEncryptionProvider = new Lazy<IProvideSymmetricEncryption>(
                    _CryptographyConfiguration.SymmetricEncryptionProviderFactory 
                        ?? (() => new SymmetricEncryptionProvider(DefaultSymmetricAlgorithm)));

                _IsConfigured = true;
            }
        }
    }
}