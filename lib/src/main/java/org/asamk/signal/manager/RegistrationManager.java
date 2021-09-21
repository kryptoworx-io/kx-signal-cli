/*
  Copyright (C) 2015-2021 AsamK and contributors

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.asamk.signal.manager;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.prefs.Preferences;

import org.asamk.signal.manager.config.ServiceConfig;
import org.asamk.signal.manager.config.ServiceEnvironment;
import org.asamk.signal.manager.config.ServiceEnvironmentConfig;
import org.asamk.signal.manager.helper.PinHelper;
import org.asamk.signal.manager.storage.SignalAccount;
import org.asamk.signal.manager.storage.identities.TrustNewIdentity;
import org.asamk.signal.manager.util.KeyUtils;
import org.asamk.signal.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.util.KeyHelper;
import org.whispersystems.libsignal.util.guava.Optional;
import org.whispersystems.signalservice.api.KeyBackupServicePinException;
import org.whispersystems.signalservice.api.KeyBackupSystemNoDataException;
import org.whispersystems.signalservice.api.SignalServiceAccountManager;
import org.whispersystems.signalservice.api.groupsv2.ClientZkOperations;
import org.whispersystems.signalservice.api.groupsv2.GroupsV2Operations;
import org.whispersystems.signalservice.api.kbs.MasterKey;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.util.UuidUtil;
import org.whispersystems.signalservice.internal.ServiceResponse;
import org.whispersystems.signalservice.internal.push.LockedException;
import org.whispersystems.signalservice.internal.push.RequestVerificationCodeResponse;
import org.whispersystems.signalservice.internal.push.VerifyAccountResponse;
import org.whispersystems.signalservice.internal.util.DynamicCredentialsProvider;

import io.kryptoworx.gcm.GcmClient;
import io.kryptoworx.gcm.GcmMessageListener;
import io.kryptoworx.gcm.http.HttpException;
import io.kryptoworx.gcm.http.OkHttpTransport;
import io.kryptoworx.signalcli.storage.HsqlAccountStore;
import okhttp3.OkHttpClient;

public class RegistrationManager implements Closeable {

    private final static Logger logger = LoggerFactory.getLogger(RegistrationManager.class);

    private SignalAccount account;
    private final PathConfig pathConfig;
    private final ServiceEnvironmentConfig serviceEnvironmentConfig;
    private final String userAgent;

    private final SignalServiceAccountManager accountManager;
    private final PinHelper pinHelper;
    private static volatile CompletableFuture<String> GCM_CHALLENGE_FUTURE;
    private static Optional<String> GCM_REGISTRATION_TOKEN = Optional.absent();
    private final static GcmClient GCM_CLIENT = createGcmClient();

    private static GcmClient createGcmClient()  {
        try {
            return tryCreateGcmClient();
        } catch (Exception e) {
            logger.error("Cannot start GCM receiver", e);
            return null;
        }
    }

    private static GcmClient tryCreateGcmClient() throws IOException {
        Preferences prefs = Preferences.userRoot().node("/io/kryptoworx/gcm");
        long androidId = prefs.getLong("androidId", 0);
        long securityToken = prefs.getLong("securityToken", 0);
        String persistentId = prefs.get("persistentId", null);
        OkHttpClient httpClient = new OkHttpClient();

        GcmClient gcmClient = new GcmClient(new OkHttpTransport(httpClient), 312334754206L);
        gcmClient.setAndroidId(androidId);
        gcmClient.setSecurityToken(securityToken);
        if (persistentId != null) {
            gcmClient.setPersistentId(persistentId);
        }
        gcmClient.setMessageListener(new GcmMessageListener() {
            @Override
            public void onMessage(String persistentId, Map<String, String> appData) {
                prefs.put("persistentId", persistentId);
                CompletableFuture<String> f = GCM_CHALLENGE_FUTURE;
                if (f == null) return;
                String challenge = appData.get("challenge");
                if (challenge != null) f.complete(challenge);
            }
        });
        try {
            GCM_REGISTRATION_TOKEN = Optional.of(gcmClient.start());
        } catch (HttpException e) {
            throw new IOException("Failed to start GCM client", e);
        }
        prefs.putLong("androidId", gcmClient.getAndroidId());
        prefs.putLong("securityToken", gcmClient.getSecurityToken());
        return gcmClient;
    }


    public RegistrationManager(
            SignalAccount account,
            PathConfig pathConfig,
            ServiceEnvironmentConfig serviceEnvironmentConfig,
            String userAgent
    ) {
        this.account = account;
        this.pathConfig = pathConfig;
        this.serviceEnvironmentConfig = serviceEnvironmentConfig;
        this.userAgent = userAgent;

        GroupsV2Operations groupsV2Operations;
        try {
            groupsV2Operations = new GroupsV2Operations(ClientZkOperations.create(serviceEnvironmentConfig.getSignalServiceConfiguration()));
        } catch (Throwable ignored) {
            groupsV2Operations = null;
        }
        this.accountManager = new SignalServiceAccountManager(serviceEnvironmentConfig.getSignalServiceConfiguration(),
                new DynamicCredentialsProvider(
                        // Using empty UUID, because registering doesn't work otherwise
                        null, account.getUsername(), account.getPassword(), SignalServiceAddress.DEFAULT_DEVICE_ID),
                userAgent,
                groupsV2Operations,
                ServiceConfig.AUTOMATIC_NETWORK_RETRY);
        final var keyBackupService = accountManager.getKeyBackupService(ServiceConfig.getIasKeyStore(),
                serviceEnvironmentConfig.getKeyBackupConfig().getEnclaveName(),
                serviceEnvironmentConfig.getKeyBackupConfig().getServiceId(),
                serviceEnvironmentConfig.getKeyBackupConfig().getMrenclave(),
                10);
        this.pinHelper = new PinHelper(keyBackupService);
    }
    
    public static RegistrationManager initInternal(String username) throws Exception{
    	String userAgent = "Signal-Android/5.22.3 signal-cli";
    	var pathConfig = PathConfig.createDefault(getDefaultDataPath());
        final var serviceConfiguration = ServiceConfig.getServiceEnvironmentConfig(ServiceEnvironment.LIVE, userAgent);
        HsqlAccountStore accountStore = SignalAccount.createAccountStore(pathConfig.getDataPath());
        if (username != null && !accountStore.userExists(username)) {
            var identityKey = KeyUtils.generateIdentityKeyPair();
            var registrationId = KeyHelper.generateRegistrationId(false);
            var profileKey = KeyUtils.createProfileKey();
            var account = SignalAccount.create(pathConfig.getDataPath(),
                    accountStore,
                    username,
                    identityKey,
                    registrationId,
                    profileKey,
                    TrustNewIdentity.ON_FIRST_USE);
            
            return new RegistrationManager(account,  pathConfig, serviceConfiguration, userAgent);
        }

        var account = SignalAccount.load(accountStore, pathConfig.getDataPath(), username, true, TrustNewIdentity.ON_FIRST_USE);
        return new RegistrationManager(account, pathConfig, serviceConfiguration, userAgent);
    }
    
    /**
     * @return the default data directory to be used by signal-cli.
     */
    private static File getDefaultDataPath() {
        return new File(IOUtils.getDataHomeDir(), "signal-cli");
    }

    public static RegistrationManager init(
            String username, File settingsPath, ServiceEnvironment serviceEnvironment, String userAgent
    ) throws IOException {
        var pathConfig = PathConfig.createDefault(settingsPath);

        final var serviceConfiguration = ServiceConfig.getServiceEnvironmentConfig(serviceEnvironment, userAgent);
        HsqlAccountStore accountStore = SignalAccount.createAccountStore(pathConfig.getDataPath());
        if (username == null || !accountStore.userExists(username)) {
            var identityKey = KeyUtils.generateIdentityKeyPair();
            var registrationId = KeyHelper.generateRegistrationId(false);

            var profileKey = KeyUtils.createProfileKey();
            var account = SignalAccount.create(pathConfig.getDataPath(),
                    accountStore, 
                    username,
                    identityKey,
                    registrationId,
                    profileKey,
                    TrustNewIdentity.ON_FIRST_USE);

            return new RegistrationManager(account, pathConfig, serviceConfiguration, userAgent);
        }

        var account = SignalAccount.load(accountStore, pathConfig.getDataPath(), username, true, TrustNewIdentity.ON_FIRST_USE);
        return new RegistrationManager(account, pathConfig, serviceConfiguration, userAgent);
    }

    private Optional<String> getGcmChallenge() throws IOException {
        if (GCM_CLIENT == null) return Optional.absent();
        GCM_CHALLENGE_FUTURE = new CompletableFuture<>();
        accountManager.requestRegistrationPushChallenge(GCM_REGISTRATION_TOKEN.get(), account.getUsername());
        try {
            return Optional.of(GCM_CHALLENGE_FUTURE.get());
        } catch (InterruptedException | ExecutionException e) {
            return Optional.absent();
        }
    }

    public void register(boolean voiceVerification, String captcha) throws IOException {
        final ServiceResponse<RequestVerificationCodeResponse> response;
        Optional<String> gcmChallenge = getGcmChallenge();
        if (voiceVerification) {
            response = accountManager.requestVoiceVerificationCode(getDefaultLocale(),
                    Optional.fromNullable(captcha),
                    gcmChallenge,
                    Optional.absent());
        } else {
            response = accountManager.requestSmsVerificationCode(false,
                    Optional.fromNullable(captcha),
                    gcmChallenge,
                    Optional.absent());
        }
        handleResponseException(response);
    }

    private Locale getDefaultLocale() {
        final var locale = Locale.getDefault();
        try {
            Locale.LanguageRange.parse(locale.getLanguage() + "-" + locale.getCountry());
        } catch (IllegalArgumentException e) {
            logger.debug("Invalid locale, ignoring: {}", locale);
            return null;
        }

        return locale;
    }

    public Manager verifyAccount(
            String verificationCode, String pin
    ) throws IOException, LockedException, KeyBackupSystemNoDataException, KeyBackupServicePinException {
        verificationCode = verificationCode.replace("-", "");
        VerifyAccountResponse response;
        MasterKey masterKey;
        try {
            response = verifyAccountWithCode(verificationCode, null);

            masterKey = null;
            pin = null;
        } catch (LockedException e) {
            if (pin == null) {
                throw e;
            }

            var registrationLockData = pinHelper.getRegistrationLockData(pin, e);
            if (registrationLockData == null) {
                throw e;
            }

            var registrationLock = registrationLockData.getMasterKey().deriveRegistrationLock();
            try {
                response = verifyAccountWithCode(verificationCode, registrationLock);
            } catch (LockedException _e) {
                throw new AssertionError("KBS Pin appeared to matched but reg lock still failed!");
            }
            masterKey = registrationLockData.getMasterKey();
        }

        //accountManager.setGcmId(Optional.of(GoogleCloudMessaging.getInstance(this).register(REGISTRATION_ID)));
        account.finishRegistration(UuidUtil.parseOrNull(response.getUuid()), masterKey, pin);

        Manager m = null;
        try {
            m = new Manager(account, pathConfig, serviceEnvironmentConfig, userAgent);
            account = null;

            m.refreshPreKeys();
            // Set an initial empty profile so user can be added to groups
            try {
                m.setProfile(null, null, null, null, null);
            } catch (NoClassDefFoundError e) {
                logger.warn("Failed to set default profile: {}", e.getMessage());
            }
            if (response.isStorageCapable()) {
                m.retrieveRemoteStorage();
            }

            final var result = m;
            m = null;

            return result;
        } finally {
            if (m != null) {
                m.close();
            }
        }
    }

    private VerifyAccountResponse verifyAccountWithCode(
            final String verificationCode, final String registrationLock
    ) throws IOException {
        final ServiceResponse<VerifyAccountResponse> response;
        if (registrationLock == null) {
            response = accountManager.verifyAccount(verificationCode,
                    account.getLocalRegistrationId(),
                    true,
                    account.getSelfUnidentifiedAccessKey(),
                    account.isUnrestrictedUnidentifiedAccess(),
                    ServiceConfig.capabilities,
                    account.isDiscoverableByPhoneNumber());
        } else {
            response = accountManager.verifyAccountWithRegistrationLockPin(verificationCode,
                    account.getLocalRegistrationId(),
                    true,
                    registrationLock,
                    account.getSelfUnidentifiedAccessKey(),
                    account.isUnrestrictedUnidentifiedAccess(),
                    ServiceConfig.capabilities,
                    account.isDiscoverableByPhoneNumber());
        }
        handleResponseException(response);
        return response.getResult().get();
    }

    @Override
    public void close() throws IOException {
        if (account != null) {
            account.close();
            account = null;
        }
    }

    private void handleResponseException(final ServiceResponse<?> response) throws IOException {
        final var throwableOptional = response.getExecutionError().or(response.getApplicationError());
        if (throwableOptional.isPresent()) {
            if (throwableOptional.get() instanceof IOException) {
                throw (IOException) throwableOptional.get();
            } else {
                throw new IOException(throwableOptional.get());
            }
        }
    }
}
