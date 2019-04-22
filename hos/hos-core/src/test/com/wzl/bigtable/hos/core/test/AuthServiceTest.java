package com.wzl.bigtable.hos.core.test;

import com.wzl.bigtable.hos.core.authmgr.IAuthService;
import com.wzl.bigtable.hos.core.authmgr.model.ServiceAuth;
import com.wzl.bigtable.hos.core.authmgr.model.TokenInfo;
import com.wzl.bigtable.hos.mybatis.test.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Date;
import java.util.List;

/**
 * Created by wzl
 */
public class AuthServiceTest extends BaseTest {

  @Autowired
  @Qualifier("authServiceImpl")
  IAuthService authService;

  @Test
  public void addToken() {
    TokenInfo tokenInfo = new TokenInfo("wzl");
    authService.addToken(tokenInfo);
  }

  @Test
  public void getTokenByUser(){
      List<TokenInfo> tokenInfos = authService.getTokenInfos("wzl");
      tokenInfos.forEach(tokenInfo -> {
          System.out.println(tokenInfo);
      });
  }

  @Test
  public void refreshToken() {
    List<TokenInfo> tokenInfos = authService.getTokenInfos("wzl");
    tokenInfos.forEach(tokenInfo -> {
      authService.refreshToken(tokenInfo.getToken());
    });
  }

  @Test
  public void deleteToken() {
    List<TokenInfo> tokenInfos = authService.getTokenInfos("wzl");
    if (tokenInfos.size() > 0) {
      authService.deleteToken(tokenInfos.get(0).getToken());
    }
  }

  @Test
  public void addAuth() {
    List<TokenInfo> tokenInfos = authService.getTokenInfos("wzl");
    if (tokenInfos.size() > 0) {
      ServiceAuth serviceAuth = new ServiceAuth();
      serviceAuth.setAuthTime(new Date());
      serviceAuth.setBucketName("testBucket");
      serviceAuth.setTargetToken(tokenInfos.get(0).getToken());
      authService.addAuth(serviceAuth);
    }
  }

  @Test
  public void deleteAuth() {
    List<TokenInfo> tokenInfos = authService.getTokenInfos("wzl");
    if (tokenInfos.size() > 0) {
      authService.deleteAuth("testBucket", tokenInfos.get(0).getToken());
    }
  }
}
