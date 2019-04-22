package com.wzl.bigtable.hos.server;

import com.wzl.bigtable.common.BucketModel;
import com.wzl.bigtable.hos.core.authmgr.IAuthService;
import com.wzl.bigtable.hos.core.authmgr.model.ServiceAuth;
import com.wzl.bigtable.hos.core.usermgr.IUserService;
import com.wzl.bigtable.hos.core.usermgr.model.SystemRole;
import com.wzl.bigtable.hos.core.usermgr.model.UserInfo;
import com.wzl.bigtable.hos.mybatis.test.BaseTest;
import com.wzl.bigtable.hos.server.dao.BucketMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class BucketMapperTest extends BaseTest {
    @Autowired
    BucketMapper bucketMapper;
    @Autowired
    @Qualifier("authServiceImpl")
    IAuthService authService;
    @Autowired
    @Qualifier("userServiceImpl")
    IUserService userService;
    @Autowired
    @Qualifier("bucketServiceImpl")
    IBucketService iBucketService;

    @Test
    public void addBucket() {
        UserInfo wzl = userService.getUserInfoByName("wzl");
        iBucketService.addBucket(wzl,"test1","this is test bucket");
        UserInfo userInfo = new UserInfo("wsj", "123456", SystemRole.ADMIN, "");
        userService.addUser(userInfo);
        ServiceAuth serviceAuth = new ServiceAuth();
        serviceAuth.setTargetToken(userInfo.getUserId());
        serviceAuth.setBucketName("test1");
        authService.addAuth(serviceAuth);
    }
}
